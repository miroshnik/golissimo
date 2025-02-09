import puppeteer from '@cloudflare/puppeteer';
import { GoogleGenerativeAI } from '@google/generative-ai';

interface Env {
	PROCESSED_POSTS_KV: KVNamespace;

	BROWSER: Fetcher;

	GOOGLE_GEMINI_API_KEY: string;
	TELEGRAM_CHAT_ID: string;
	TELEGRAM_BOT_TOKEN: string;
}

const prompt = `мне нужен ответ в виде хештегов через пробел для строки о событии в матче, где будут команды и игроки или игрок, если он один;
пример: для строки "Metz 1-0 Grenoble - Pape Amadou Diallo 42'" ответ будет: #Metz #Grenoble #PapeAmadouDiallo;
пример: для строки "Léo Pelé (Athletico-PR) faces racist insults after being sent off against Coritiba" ответ будет: #AthleticoPR #Coritiba #LéoPelé;
так же нужны теги для самого события, типа ВАР, удаление, пенальти, красная карта, гол, травма и т.д.; OG - own goal;
пример: Milan 2-2 Parma - Strahinja Pavlović VAR Disallow -> #Milan #Parma #StrahinjaPavlović #VAR #GoalDiasallowed;
в хештегах должны быть только буквы;
если есть только фамилия игрока или только клуб, то нужны только соответствующие хештеги;
сначала команды, потом игроки;
ответ только на английском;
строка: `;

export default {
	async fetch (request, env, ctx): Promise<Response> {
		return Response.json(await env.PROCESSED_POSTS_KV.list());
	},

	async scheduled (event, env) {
		try {
			const genAI = new GoogleGenerativeAI(env.GOOGLE_GEMINI_API_KEY);
			const model = genAI.getGenerativeModel({ model: 'gemini-2.0-flash-exp' });

			const response = await fetch('https://www.reddit.com/r/soccer/new.json?limit=25', {
				headers: { 'User-Agent': 'Mozilla/5.0' }
			});

			if (!response.ok) {
				console.error(`Posts fetch error: ${response.status} ${response.statusText}`);

				return;
			}

			const data: Record<any, any> = await response.json();

			for (const item of data.data.children.filter((post: any) => post.data.link_flair_text === 'Media').reverse()) {
				const id = item.data.id;
				const title = item.data.title;
				let videoUrl = item.data.media?.reddit_video?.fallback_url || item.data.url;

				const retriesLeft = Number(await env.PROCESSED_POSTS_KV.get(id) ?? 5); // 5 retries

				if (retriesLeft <= 0) {
					continue;
				}

				console.log(`ID: ${id}, Title: ${title}. Processing.`);

				if (videoUrl && !(
					videoUrl.includes('youtube') || videoUrl.includes('youtu.be') || videoUrl.includes('.jpeg') || videoUrl.includes('.png')
					|| ((videoUrl.includes('.m3u8') || videoUrl.includes('.mp4')) && !videoUrl.includes('DASH_96.')))
				) {
					console.log(`Open ${videoUrl} in browser...`);
					videoUrl = await getFinalStreamUrl(env, videoUrl);
				}

				console.log(`Video URL: ${videoUrl}, ID: ${id}`);

				if (videoUrl) {
					let message = `${title} <a href="${videoUrl}">↗</a>`;
					if ((videoUrl.includes('.mp4') || videoUrl.includes('.m3u8')) && !videoUrl.includes('DASH_96.')) {
						message += ` <a href="https://demo.meshkov.info/video?url=${encodeURIComponent(videoUrl)}">▷</a>`;
					}
					message += `\n${(await model.generateContent(prompt + title)).response.text()}`;

					const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
						method: 'POST',
						headers: {
							'Content-Type': 'application/json'
						},
						body: JSON.stringify({
							chat_id: env.TELEGRAM_CHAT_ID,
							text: message,
							parse_mode: 'HTML'
						})
					});

					if (!response.ok) {
						console.error(`Failed to send Telegram message: ${response.status}`);
						continue;
					}

					await env.PROCESSED_POSTS_KV.put(id, '0', { expirationTtl: 24 * 60 * 60 }); // store for 1 day (in seconds)
				} else {
					await env.PROCESSED_POSTS_KV.put(id, (retriesLeft-1).toString(), { expirationTtl: 24 * 60 * 60 }); // store for 1 day (in seconds)
				}
			}
		} catch (error) {
			console.error(error);
		}
	}
} satisfies ExportedHandler<Env>;

const getFinalStreamUrl = async (env: Env, streamUrl: string): Promise<string | null> => {
	const browser = await puppeteer.launch(env.BROWSER);

	const page = await browser.newPage();

	await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36');

	let finalStreamUrl: string | null = null;
	try {
		await page.setRequestInterception(true);

		page.on('request', async (request) => {
			const url = request.url();

			if (!finalStreamUrl && ((url.includes('.m3u8') || url.includes('.mp4')) && !url.includes('DASH_96.'))) {
				console.log(`Video stream found: ${url}`);
				finalStreamUrl = url;
			} else {
				await request.continue();
			}
		});

		await page.goto(streamUrl, { waitUntil: 'domcontentloaded' });

		await page.waitForSelector('video source', { timeout: 3000 }); // Waiting for video.stream element

		return finalStreamUrl;
	} catch (error) {
		console.error(`Stream processing fail for ${streamUrl}:`, error);

		return finalStreamUrl;
	} finally {
		await browser.close();
	}
};
