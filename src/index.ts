import puppeteer from '@cloudflare/puppeteer';

interface Env {
	PROCESSED_POSTS_KV: KVNamespace;
	BROWSER: Fetcher;
	GEMINI_API_KEY: string;
	TELEGRAM_CHAT_ID: string;
	TELEGRAM_BOT_TOKEN: string;
}

export default {
	async fetch (request, env, ctx): Promise<Response> {
		return Response.json(await env.PROCESSED_POSTS_KV.list());
	},

	async scheduled (event, env) {
		try {
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

				const retriesLeft = Number(await env.PROCESSED_POSTS_KV.get(id) ?? 5);
				if (retriesLeft <= 0) continue;

				console.log(`ID: ${id}, Title: ${title}. Processing.`);

				if (videoUrl && !(
					videoUrl.includes('youtube') || videoUrl.includes('youtu.be') || videoUrl.includes('.jpeg') || videoUrl.includes('.png')
					|| ((videoUrl.includes('.m3u8') || videoUrl.includes('.mp4')) && !videoUrl.includes('DASH_96.'))
				)) {
					console.log(`Open ${videoUrl} in browser...`);
					videoUrl = await getFinalStreamUrl(env, videoUrl);
				}

				console.log(`Video URL: ${videoUrl}, ID: ${id}`);

				if (videoUrl) {
					const prompt = `
Extract hashtags from a match event string.

Rules:
- Only include team names and player names (with letters only, no numbers or symbols).
- Add relevant event hashtags (e.g. #Goal, #RedCard, #Penalty, #Injury, #VAR, etc.).
- First teams, then players, then event type.
- If there are no teams or players, return nothing at all (not even event tags).
- Return only hashtags, space-separated. No comments, no extra text.

Examples:
Input: Annecy 1-0 Caen - Yohann Demoncy 13'
Output: #Annecy #Caen #YohannDemoncy #Goal

Input: Rayo Vallecano disallowed goal against Barcelona 42'
Output: #RayoVallecano #Barcelona #GoalDisallowed

Input: Choreography Display During Hammarby IF vs Djurgårdens IF Match
Output: #HammarbyIF #DjurgardensIF #ChoreographyDisplay

Input: Final whistle
Output:

Input:
`;

					let message = `${title} <a href="${videoUrl}">↗</a>`;
					if ((videoUrl.includes('.mp4') || videoUrl.includes('.m3u8')) && !videoUrl.includes('DASH_96.')) {
						message += ` <a href="https://demo.meshkov.info/video?url=${encodeURIComponent(videoUrl)}">▷</a>`;
					}

					const geminiResponse = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${env.GEMINI_API_KEY}`, {
						method: 'POST',
						headers: {
							'Content-Type': 'application/json'
						},
						body: JSON.stringify({
							contents: [
								{
									role: 'user',
									parts: [{ text: prompt + title }]
								}
							]
						})
					});

					const geminiJson = await geminiResponse.json() as any;
					const aiText = geminiJson.candidates?.[0]?.content?.parts?.[0]?.text ?? '';
					message += `\n${aiText}`;

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

					await env.PROCESSED_POSTS_KV.put(id, '0', { expirationTtl: 86400 });
				} else {
					await env.PROCESSED_POSTS_KV.put(id, (retriesLeft - 1).toString(), { expirationTtl: 86400 });
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
		await page.waitForSelector('video source', { timeout: 3000 });
		return finalStreamUrl;
	} catch (error) {
		console.error(`Stream processing fail for ${streamUrl}:`, error);
		return finalStreamUrl;
	} finally {
		await browser.close();
	}
};
