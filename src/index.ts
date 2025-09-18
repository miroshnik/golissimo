import puppeteer from '@cloudflare/puppeteer';

interface Env {
	PROCESSED_POSTS_KV: KVNamespace;
	BROWSER: Fetcher;
	GEMINI_API_KEY: string;
	TELEGRAM_CHAT_ID: string;
	TELEGRAM_BOT_TOKEN: string;
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/player') {
			const video = url.searchParams.get('video') || '';
			const audio = url.searchParams.get('audio') || '';
			const html = `<!doctype html>
			<html lang="en">
			<head>
			<meta charset="utf-8" />
			<meta name="viewport" content="width=device-width, initial-scale=1" />
			<title>Player</title>
			<style>
				html, body { margin:0; padding:0; background:#000; height:100%; width:100%; }
				#wrap { position:fixed; inset:0; }
				video { height:100%; width:100%; object-fit:contain; background:#000; display:block; }
			</style>
			</head>
			<body>
				<div id="wrap">
					<video id="v" playsinline controls preload="metadata" src="${escapeHtml(video)}"></video>
					<audio id="a" preload="metadata" src="${escapeHtml(audio)}" ${audio ? '' : 'style="display:none"'}></audio>
				</div>
				<script>
				(function(){
					const v = document.getElementById('v');
					const a = document.getElementById('a');
					if (!a.src) return;
					let syncing = false;
					function sync(from, to){
						if (syncing) return; syncing = true; try { to.currentTime = from.currentTime; } catch(e){}
						setTimeout(()=>{ syncing = false; }, 0);
					}
					v.addEventListener('play', ()=>{ a.play().catch(()=>{}); sync(v,a); });
					v.addEventListener('pause', ()=>{ a.pause(); });
					v.addEventListener('seeking', ()=> sync(v,a));
					v.addEventListener('ratechange', ()=>{ a.playbackRate = v.playbackRate; });
					a.addEventListener('seeking', ()=> sync(a,v));
				})();
				</script>
			</body>
			</html>`;
			return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
		}
		return Response.json(await env.PROCESSED_POSTS_KV.list());
	},

	async scheduled(event, env) {
		try {
			const data = await fetchRedditNew(env);
			if (!data) return;

			const processedThisRun = new Set<string>();

			for (const item of data.data.children.filter((post: any) => post.data.link_flair_text === 'Media').reverse()) {
				const key = derivePostKey(item);
				if (processedThisRun.has(key)) continue;
				const title = item.data.title;
				let videoUrl = extractBestMediaUrl(item.data);
				let audioUrl: string | null = null;
				// Try prefer hls_url (muxed av) if available under media.reddit_video
				const hls = item.data?.media?.reddit_video?.hls_url as string | undefined;
				if (hls) {
					videoUrl = hls;
				} else if (videoUrl && videoUrl.includes('DASH_') && videoUrl.endsWith('.mp4')) {
					// derive audio from DASH_XXX.mp4 -> DASH_audio.mp4
					audioUrl = videoUrl.replace(/DASH_[^/.]+\.mp4$/, 'DASH_audio.mp4');
				}

				// Skip immediately if post already fully processed previously
				const retriesLeft = Number((await env.PROCESSED_POSTS_KV.get(key)) ?? 5);
				if (retriesLeft <= 0) continue;

				// Early media-level dedupe before any heavy work
				if (videoUrl) {
					const rawMediaKey = `media:${canonicalizeUrl(videoUrl)}`;
					if (processedThisRun.has(rawMediaKey) || (await env.PROCESSED_POSTS_KV.get(rawMediaKey))) {
						await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
						continue;
					}
					processedThisRun.add(rawMediaKey);
				}

				console.log(`Key: ${key}, Title: ${title}. Processing.`);

				if (
					videoUrl &&
					!(
						videoUrl.includes('youtube') ||
						videoUrl.includes('youtu.be') ||
						isDirectImageUrl(videoUrl) ||
						((videoUrl.includes('.m3u8') || videoUrl.includes('.mp4')) && !videoUrl.includes('DASH_96.'))
					)
				) {
					console.log(`Open ${videoUrl} in browser...`);
					const resolved = await getFinalStreamUrl(env, videoUrl);
					if (resolved) videoUrl = resolved;
				}

				console.log(`Video URL: ${videoUrl}, Key: ${key}`);

				if (videoUrl) {
					const mediaKey = `media:${canonicalizeUrl(videoUrl)}`;
					if (processedThisRun.has(mediaKey) || (await env.PROCESSED_POSTS_KV.get(mediaKey))) {
						// Mark post as processed to avoid future retries for the same duplicate
						await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
						continue;
					}

					processedThisRun.add(mediaKey);
					const prompt = `
					Извлеки хештеги из строки
Только хештеги, через пробел. Никаких комментариев и кода.

Правила:
- Только названия команд и игроков (только буквы и цифры).
- Добавь тег события (например, #Goal, #RedCard, #Interview и т.д.).
- Порядок: команды → игроки → событие.
- Если нет команд и игроков — выбери ключевые теги по смыслу.

Пример:
Annecy 1-0 Caen - Yohann Demoncy 13'
#Annecy #Caen #YohannDemoncy #Goal

Теперь ответь для моей строки:
`;

					const safeTitle = escapeHtml(title);
					let message = `${safeTitle} <a href="${escapeHtml(videoUrl)}">↗</a>`;
					if (videoUrl.includes('.mp4') || videoUrl.includes('.m3u8')) {
						const playerUrl = `/player?video=${encodeURIComponent(videoUrl)}${audioUrl ? `&audio=${encodeURIComponent(audioUrl)}` : ''}`;
						const absPlayerUrl = new URL(playerUrl, 'https://golissimo.workers.dev').toString();
						message += ` <a href="${escapeHtml(absPlayerUrl)}">▷</a>`;
					}

					const geminiResponse = await fetch(
						`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${env.GEMINI_API_KEY}`,
						{
							method: 'POST',
							headers: {
								'Content-Type': 'application/json',
							},
							body: JSON.stringify({
								contents: [
									{
										role: 'user',
										parts: [{ text: prompt + title }],
									},
								],
							}),
						}
					);

					const geminiJson = (await geminiResponse.json()) as any;

					console.log(geminiJson);

					const aiText = geminiJson.candidates?.[0]?.content?.parts?.[0]?.text ?? '';
					message += `\n${aiText}`;

					const isImage = isDirectImageUrl(videoUrl);
					const tgMethod = isImage ? 'sendPhoto' : 'sendMessage';
					const tgPayload = isImage
						? { chat_id: env.TELEGRAM_CHAT_ID, photo: videoUrl, caption: message, parse_mode: 'HTML' }
						: { chat_id: env.TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' };
					const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${tgMethod}`, {
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						body: JSON.stringify(tgPayload),
					});

					if (!response.ok) {
						let bodyText = '';
						try {
							bodyText = await response.text();
						} catch {}
						console.error(`Failed to send Telegram message: ${response.status} ${response.statusText} - ${bodyText}`);
						continue;
					}

					await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
					await env.PROCESSED_POSTS_KV.put(mediaKey, '1', { expirationTtl: 604800 });
				} else {
					await env.PROCESSED_POSTS_KV.put(key, (retriesLeft - 1).toString(), { expirationTtl: 604800 });
				}
			}
		} catch (error) {
			console.error(error);
		}
	},
} satisfies ExportedHandler<Env>;

const derivePostKey = (item: any): string => {
	const d = item?.data ?? {};
	// Prefer stable unique fields from Reddit listing
	const candidates: string[] = [
		d.name, // e.g., t3_abcdef
		d.id, // short id
		d.permalink,
		d.url,
		`${d.title || ''}::${d.created_utc || ''}`,
	].filter(Boolean);
	const raw = candidates[0] as string;
	// Normalize URLs and whitespace
	try {
		if (raw.startsWith('http')) {
			const u = new URL(raw);
			u.search = '';
			u.hash = '';
			return `post:${u.toString()}`;
		}
	} catch {}
	return `post:${raw}`;
};

const fetchRedditNew = async (env: Env): Promise<any | null> => {
	const url = 'https://api.reddit.com/r/soccer/new?limit=25';
	const response = await fetch(url, { headers: { 'User-Agent': 'golissimo bot 1.0' } });
	if (response.status === 429) {
		console.error('Posts fetch error: 429 Too Many Requests');
		return null;
	}
	if (!response.ok) {
		console.error(`Posts fetch error: ${response.status} ${response.statusText}`);
		return null;
	}
	return response.json();
};

const canonicalizeUrl = (rawUrl: string): string => {
	try {
		const u = new URL(rawUrl);
		// Strip query/hash and unify hostname case
		u.search = '';
		u.hash = '';
		u.hostname = u.hostname.toLowerCase();
		// Optionally normalize trailing slashes
		u.pathname = u.pathname.replace(/\/+$/, '');
		return `${u.protocol}//${u.hostname}${u.pathname}`;
	} catch {
		return rawUrl;
	}
};

const extractBestMediaUrl = (d: any): string | null => {
	// 1) Native Reddit video
	const video = d?.media?.reddit_video?.fallback_url;
	if (video) return String(video);

	// 2) Gallery posts (multiple images)
	if (d?.is_gallery && d?.gallery_data?.items && d?.media_metadata) {
		for (const it of d.gallery_data.items as any[]) {
			const meta = d.media_metadata?.[it.media_id];
			if (!meta) continue;
			// Prefer original (s), fallback to largest preview (p)
			const src = meta.s?.u || meta.s?.gif || (Array.isArray(meta.p) && meta.p.length ? meta.p[meta.p.length - 1]?.u : null);
			if (src) return decodeRedditUrl(String(src));
		}
	}

	// 3) Preview (image/gif/mp4)
	const pimg = d?.preview?.images?.[0];
	if (pimg) {
		const mp4 = pimg.variants?.mp4?.source?.url;
		if (mp4) return decodeRedditUrl(String(mp4));
		const gif = pimg.variants?.gif?.source?.url;
		if (gif) return decodeRedditUrl(String(gif));
		const src = pimg.source?.url;
		if (src) return decodeRedditUrl(String(src));
	}

	// 4) Overridden URL or direct image
	const overridden = d?.url_overridden_by_dest;
	if (overridden) return String(overridden);
	const url = d?.url;
	if (url && /(\.jpe?g|\.png|\.gif|\.mp4|\.m3u8)(\?|$)/i.test(url)) return String(url);

	return null;
};

const decodeRedditUrl = (u: string): string => u.replace(/&amp;/g, '&');

const escapeHtml = (input: string): string => {
	return String(input).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
};

const getFinalStreamUrl = async (env: Env, streamUrl: string): Promise<string | null> => {
	const browser = await puppeteer.launch(env.BROWSER);
	const page = await browser.newPage();

	await page.setUserAgent(
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
	);

	let finalStreamUrl: string | null = null;
	try {
		await page.setRequestInterception(true);

		page.on('request', async (request) => {
			const url = request.url();
			if (!finalStreamUrl && isMediaUrl(url)) {
				console.log(`Video stream found (request): ${url}`);
				finalStreamUrl = url;
			}
			await request.continue();
		});

		page.on('response', async (response) => {
			const url = response.url();
			if (!finalStreamUrl && isMediaUrl(url)) {
				console.log(`Video stream found (response): ${url}`);
				finalStreamUrl = url;
			}
		});

		try {
			await page.goto(streamUrl, { waitUntil: 'networkidle2' });
		} catch (_) {
			await page.goto(streamUrl, { waitUntil: 'domcontentloaded' });
		}

		if (!finalStreamUrl) {
			try {
				const req = await page.waitForRequest((req) => isMediaUrl(req.url()), { timeout: 10000 });
				finalStreamUrl = req.url();
			} catch (_) {
				// ignore
			}
		}

		if (!finalStreamUrl) {
			await new Promise((r) => setTimeout(r, 2000));
		}

		return finalStreamUrl;
	} catch (error) {
		console.error(`Stream processing fail for ${streamUrl}:`, error);
		return finalStreamUrl;
	} finally {
		await browser.close();
	}
};

const isMediaUrl = (url: string): boolean => {
	return (url.includes('.m3u8') || url.includes('.mp4')) && !url.includes('DASH_96.');
};

const isDirectImageUrl = (url: string): boolean => {
	try {
		const u = new URL(url);
		const lower = u.href.toLowerCase();
		if (u.hostname.endsWith('preview.redd.it')) return true;
		return /\.(jpe?g|png|webp|gif)(\?|$)/i.test(lower) || lower.includes('format=pjpg');
	} catch {
		return /\.(jpe?g|png|webp|gif)(\?|$)/i.test(url.toLowerCase());
	}
};
