// Small logging helpers (logfmt to avoid UI redaction noise)
const log = (event: string, data?: Record<string, unknown>) => {
	const parts: string[] = [`evt=${event}`];
	if (data) {
		for (const [k, v] of Object.entries(data)) {
			let val = String(v ?? '');
			val = val.replace(/\s+/g, '_').slice(0, 160);
			parts.push(`${k}=${val}`);
		}
	}
	console.log(parts.join(' '));
};

const shortUrl = (u: string | null): string | null => {
	if (!u) return u;
	try {
		const x = new URL(u);
		x.search = '';
		return x.toString();
	} catch {
		return u;
	}
};
import puppeteer from '@cloudflare/puppeteer';

interface Env {
	PROCESSED_POSTS_KV: KVNamespace;
	BROWSER: Fetcher;
	TELEGRAM_CHAT_ID: string;
	TELEGRAM_BOT_TOKEN: string;
	AI: Ai;
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/admin/kv/clear') {
			if (request.method !== 'POST') return new Response('Method Not Allowed', { status: 405 });
			const token = request.headers.get('x-admin-token') || '';
			if (token !== env.TELEGRAM_BOT_TOKEN) return new Response('Unauthorized', { status: 401 });
			const prefix = url.searchParams.get('prefix') || '';
			if (!prefix) return new Response('Bad Request: prefix required (post:, media:, all)', { status: 400 });
			if (prefix === 'all') {
				const deletedPosts = await clearKvPrefix(env, 'post:');
				const deletedMedia = await clearKvPrefix(env, 'media:');
				return Response.json({ deleted: deletedPosts + deletedMedia, details: { post: deletedPosts, media: deletedMedia } });
			}
			const deleted = await clearKvPrefix(env, prefix);
			return Response.json({ deleted, prefix });
		}
		if (url.pathname === '/proxy') {
			const src = url.searchParams.get('url') || '';
			try {
				const u = new URL(src);
				if (u.protocol !== 'http:' && u.protocol !== 'https:') {
					return new Response('Bad Request', { status: 400 });
				}
				const resp = await fetch(u.toString(), {
					headers: { 'User-Agent': 'Mozilla/5.0' },
					cf: { cacheTtl: 300, cacheEverything: true },
				});
				const headers = new Headers(resp.headers);
				headers.set('Cache-Control', 'public, max-age=300');
				headers.delete('Set-Cookie');
				return new Response(resp.body, { status: resp.status, headers });
			} catch {
				return new Response('Bad Request', { status: 400 });
			}
		}
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
					<video id="v" playsinline autoplay muted controls preload="metadata" src="${escapeHtml(video)}"></video>
					<audio id="a" preload="metadata" src="${escapeHtml(audio)}" ${audio ? '' : 'style="display:none"'}></audio>
				</div>
				<script>
				(function(){
					const v = document.getElementById('v');
					const a = document.getElementById('a');
					// Autoplay: keep both muted to satisfy browser policies
					if (a && a.src) { a.muted = true; a.play().catch(()=>{}); }
					v.play().catch(()=>{});
					let syncing = false;
					function sync(from, to){
						if (!to) return; if (syncing) return; syncing = true; try { to.currentTime = from.currentTime; } catch(e){}
						setTimeout(()=>{ syncing = false; }, 0);
					}
					if (a && a.src) {
						v.addEventListener('play', ()=>{ a.play().catch(()=>{}); sync(v,a); });
						v.addEventListener('pause', ()=>{ a.pause(); });
						v.addEventListener('seeking', ()=> sync(v,a));
						v.addEventListener('ratechange', ()=>{ a.playbackRate = v.playbackRate; });
						a.addEventListener('seeking', ()=> sync(a,v));
					}
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
			log('scheduled:start');
			const data = await fetchRedditNew(env);
			if (!data) {
				log('scheduled:no-data');
				return;
			}

			const processedThisRun = new Set<string>();
			log('scheduled:children', { count: data.data?.children?.length });

			for (const item of data.data.children.filter((post: any) => post.data.link_flair_text === 'Media').reverse()) {
				const key = derivePostKey(item);
				if (processedThisRun.has(key)) {
					log('skip:already-in-run', { key });
					continue;
				}
				const title = item.data.title;
				let videoUrl = extractBestMediaUrl(item.data);
				let audioUrl: string | null = null;
				// Try prefer hls_url (muxed av) if available under media.reddit_video
				const hls = item.data?.media?.reddit_video?.hls_url as string | undefined;
				if (hls) {
					videoUrl = hls;
					log('media:hls', { key, url: shortUrl(videoUrl) });
				} else if (videoUrl && videoUrl.includes('DASH_') && videoUrl.endsWith('.mp4')) {
					// derive audio from DASH_XXX.mp4 -> DASH_audio.mp4
					audioUrl = videoUrl.replace(/DASH_[^/.]+\.mp4$/, 'DASH_audio.mp4');
					log('media:dash', { key, video: shortUrl(videoUrl), audio: shortUrl(audioUrl) });
				}

				// Skip immediately if post already processed or being processed by another run
				const existingKeyVal = await env.PROCESSED_POSTS_KV.get(key);
				let retriesLeft = 5;
				if (existingKeyVal !== null) {
					if (existingKeyVal === '0' || existingKeyVal === 'pending') {
						log('skip:kv-post', { key, state: existingKeyVal });
						continue;
					}
					retriesLeft = Number(existingKeyVal);
					if (!Number.isFinite(retriesLeft)) retriesLeft = 0;
				}
				log('kv:post', { key, retriesLeft });

				// Early media-level dedupe and reservation before any heavy work
				let rawMediaKey: string | null = null;
				if (videoUrl) {
					rawMediaKey = `media:${canonicalizeUrl(videoUrl)}`;
					if (processedThisRun.has(rawMediaKey) || (await env.PROCESSED_POSTS_KV.get(rawMediaKey))) {
						log('skip:kv-media', { key, mediaKey: rawMediaKey });
						continue;
					}
					processedThisRun.add(rawMediaKey);
					// Reserve both keys as pending to avoid race duplicates
					await env.PROCESSED_POSTS_KV.put(key, 'pending', { expirationTtl: 604800 });
					await env.PROCESSED_POSTS_KV.put(rawMediaKey, 'pending', { expirationTtl: 604800 });
					log('reserve:pending', { key, mediaKey: rawMediaKey });
				}

				log('process:start', { key, title });

				if (
					videoUrl &&
					!(
						videoUrl.includes('youtube') ||
						videoUrl.includes('youtu.be') ||
						isDirectImageUrl(videoUrl) ||
						((videoUrl.includes('.m3u8') || videoUrl.includes('.mp4')) && !videoUrl.includes('DASH_96.'))
					)
				) {
					log('resolve:open', { key, url: shortUrl(videoUrl) });
					const resolved = await getFinalStreamUrl(env, videoUrl);
					if (resolved) videoUrl = resolved;
					log('resolve:done', { key, url: shortUrl(videoUrl) });
				}

				log('media:final', { key, url: shortUrl(videoUrl), isImage: !!(videoUrl && isDirectImageUrl(videoUrl)) });

				if (videoUrl) {
					const mediaKey = `media:${canonicalizeUrl(videoUrl)}`;
					// Only re-check dedupe if final mediaKey differs from early reserved rawMediaKey
					if (!rawMediaKey || mediaKey !== rawMediaKey) {
						if (processedThisRun.has(mediaKey) || (await env.PROCESSED_POSTS_KV.get(mediaKey))) {
							log('skip:kv-media-final', { key, mediaKey });
							continue;
						}
					}

					processedThisRun.add(mediaKey);
					// If we reserved a raw key and final mediaKey differs, move reservation
					if (rawMediaKey && rawMediaKey !== mediaKey) {
						await env.PROCESSED_POSTS_KV.delete(rawMediaKey);
						await env.PROCESSED_POSTS_KV.put(mediaKey, 'pending', { expirationTtl: 604800 });
						log('reserve:move', { key, from: rawMediaKey, to: mediaKey });
					}
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

					try {
						const safeTitle = escapeHtml(title);
						let message = `${safeTitle} <a href="${escapeHtml(videoUrl)}">↗</a>`;
						if (videoUrl.includes('.mp4') || videoUrl.includes('.m3u8')) {
							const playerUrl = `/player?video=${encodeURIComponent(videoUrl)}${audioUrl ? `&audio=${encodeURIComponent(audioUrl)}` : ''}`;
							const absPlayerUrl = new URL(playerUrl, 'https://golissimo.miroshnik.workers.dev').toString();
							message += ` <a href="${escapeHtml(absPlayerUrl)}">▷</a>`;
						}

						let aiText = '';
						log('ai:start', { key });
						const aiResp = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
							messages: [
								{ role: 'system', content: 'Ты — краткая система генерации хештегов. Возвращай только хештеги одной строкой.' },
								{ role: 'user', content: prompt + title },
							],
						});
						const aiObj = aiResp as { response?: string };
						aiText = aiObj.response ?? '';
						if (aiText) log('ai:ok', { key });
						if (aiText) {
							// Normalize hashtags: remove dots, collapse spaces, allow only letters/digits in tag body
							aiText = aiText
								.replace(/\./g, '')
								.split(/\s+/)
								.filter(Boolean)
								.map((tok) => {
									if (!tok.startsWith('#')) tok = '#' + tok;
									// keep # then strip non-alnum from the rest
									return '#' + tok.slice(1).replace(/[^\p{L}\p{N}]/gu, '');
								})
								.join(' ');
							message += `\n${aiText}`;
						}

						const isImage = isDirectImageUrl(videoUrl);
						const tgMethod = isImage ? 'sendPhoto' : 'sendMessage';
						const photoUrl = isImage
							? new URL(`/proxy?url=${encodeURIComponent(videoUrl)}`, 'https://golissimo.miroshnik.workers.dev').toString()
							: undefined;
						const tgPayload = isImage
							? { chat_id: env.TELEGRAM_CHAT_ID, photo: photoUrl, caption: message, parse_mode: 'HTML' }
							: { chat_id: env.TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' };
						log('tg:send', { key, method: tgMethod, photo: photoUrl ? shortUrl(photoUrl) : undefined });
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
							console.error('tg:error', { status: response.status, statusText: response.statusText, body: bodyText.slice(0, 500) });
							// Release reservations on failure to allow retry later
							await env.PROCESSED_POSTS_KV.delete(mediaKey);
							await env.PROCESSED_POSTS_KV.delete(key);
							continue;
						}
						log('tg:ok', { key });
					} catch (e) {
						console.error('pipeline:error', e);
						await env.PROCESSED_POSTS_KV.delete(mediaKey);
						await env.PROCESSED_POSTS_KV.delete(key);
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

// No durable object lock

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

const clearKvPrefix = async (env: Env, prefix: string): Promise<number> => {
	let cursor: string | undefined = undefined;
	let total = 0;
	while (true) {
		const l: KVNamespaceListResult<unknown, string> = await env.PROCESSED_POSTS_KV.list({ prefix, cursor });
		if (l.keys.length) {
			await Promise.all(l.keys.map((k: KVNamespaceListKey<unknown, string>) => env.PROCESSED_POSTS_KV.delete(k.name)));
			total += l.keys.length;
		}
		if (l.list_complete) break;
		cursor = l.cursor;
	}
	return total;
};

const fetchRedditNew = async (env: Env): Promise<any | null> => {
	const url = 'https://api.reddit.com/r/soccer/new?limit=25';
	log('fetch:start', { url });
	const response = await fetch(url, { headers: { 'User-Agent': 'golissimo bot 1.0' } });
	if (response.status === 429) {
		console.error('fetch:429');
		return null;
	}
	if (!response.ok) {
		console.error('fetch:error', { status: response.status, statusText: response.statusText });
		return null;
	}
	const json = await response.json();
	log('fetch:ok');
	return json;
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

		log('resolve:return', { url: shortUrl(finalStreamUrl) });
		return finalStreamUrl;
	} catch (error) {
		console.error('resolve:error', { src: shortUrl(streamUrl), error: String(error) });
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
