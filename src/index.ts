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
			// Handle CORS preflight
			if (request.method === 'OPTIONS') {
				return new Response(null, {
					status: 204,
					headers: {
						'Access-Control-Allow-Origin': '*',
						'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
						'Access-Control-Allow-Headers': 'Range',
						'Access-Control-Max-Age': '86400',
					},
				});
			}
			// Proxy endpoint to bypass CORS for video/audio resources
			const targetUrl = url.searchParams.get('url');
			if (!targetUrl) {
				return new Response('Bad Request: url parameter required', { status: 400 });
			}
			try {
				const target = new URL(targetUrl);
				// Get Range header if present (for video seeking)
				const range = request.headers.get('range');
				const headers: HeadersInit = {
					'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
					Accept: '*/*',
				};
				if (range) {
					headers['Range'] = range;
				}
				const response = await fetch(targetUrl, {
					method: request.method,
					headers,
					redirect: 'follow',
				});
				// Create new response with CORS headers
				const corsHeaders = new Headers(response.headers);
				corsHeaders.set('Access-Control-Allow-Origin', '*');
				corsHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
				corsHeaders.set('Access-Control-Allow-Headers', 'Range');
				corsHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges');
				// Preserve important headers
				if (response.headers.get('content-type')) {
					corsHeaders.set('content-type', response.headers.get('content-type')!);
				}
				if (response.headers.get('content-length')) {
					corsHeaders.set('content-length', response.headers.get('content-length')!);
				}
				if (response.headers.get('content-range')) {
					corsHeaders.set('content-range', response.headers.get('content-range')!);
				}
				if (response.headers.get('accept-ranges')) {
					corsHeaders.set('accept-ranges', response.headers.get('accept-ranges')!);
				}
				return new Response(response.body, {
					status: response.status,
					statusText: response.statusText,
					headers: corsHeaders,
				});
			} catch (error) {
				log('proxy:error', { url: shortUrl(targetUrl || ''), error: String(error).slice(0, 100) });
				return new Response('Proxy Error: ' + String(error), { status: 500 });
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
					<video id="v" playsinline autoplay muted controls preload="metadata" crossorigin="anonymous"></video>
					<audio id="a" preload="metadata" src="${escapeHtml(audio)}" ${audio ? '' : 'style="display:none"'}></audio>
				</div>
				<script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.7/dist/hls.min.js"></script>
				<script>
				(function(){
					const v = document.getElementById('v');
					const a = document.getElementById('a');
					// Extract video URL from query string manually to handle URLs with query params
					const fullUrl = location.href;
					const queryStart = fullUrl.indexOf('?');
					let src = '';
					let audioSrc = '';
					if (queryStart !== -1) {
						const queryString = fullUrl.substring(queryStart + 1);
						// Find 'video=' parameter start
						const videoIdx = queryString.indexOf('video=');
						if (videoIdx === 0) {
							// video is first parameter
							const videoPart = queryString.substring(6); // 'video=' is 6 chars
							// Find where video URL ends (before '&audio=' or end of string)
							const audioIdx = videoPart.indexOf('&audio=');
							const videoUrlEncoded = audioIdx !== -1 ? videoPart.substring(0, audioIdx) : videoPart;
							// Decode URL
							try {
								src = decodeURIComponent(videoUrlEncoded);
								// Extract audio if present
								if (audioIdx !== -1) {
									const audioPart = videoPart.substring(audioIdx + 7); // '&audio=' is 7 chars
									try {
										audioSrc = decodeURIComponent(audioPart);
									} catch (e) {
										audioSrc = '';
									}
								}
							} catch (e) {
								// If decode fails, try searchParams as fallback
								const params = new URL(location.href).searchParams;
								src = params.get('video') || '';
								audioSrc = params.get('audio') || '';
							}
						} else {
							// Fallback to searchParams
							const params = new URL(location.href).searchParams;
							src = params.get('video') || '';
							audioSrc = params.get('audio') || '';
						}
					} else {
						// No query string, no video
						src = '';
						audioSrc = '';
					}
					// Check if URL needs CORS proxy (different origin) - do this BEFORE setting src
					let finalSrc = src;
					try {
						const srcUrl = new URL(src);
						const currentOrigin = location.origin;
						// If different origin, use proxy
						if (srcUrl.origin !== currentOrigin && !src.startsWith('/proxy?url=')) {
							finalSrc = '/proxy?url=' + encodeURIComponent(src);
							console.log('Player: Using proxy for CORS:', finalSrc);
						}
					} catch (e) {
						// If URL parsing fails, use original src
						console.log('Player: URL parse failed, using original src');
						finalSrc = src;
					}
					// Setup source with HLS.js for Chrome
					if (finalSrc.endsWith('.m3u8')) {
						if (window.Hls && Hls.isSupported()) {
							const hls = new Hls({ lowLatencyMode: true, backBufferLength: 60 });
							hls.loadSource(finalSrc);
							hls.attachMedia(v);
							hls.on(Hls.Events.MANIFEST_PARSED, ()=>{ v.play().catch(()=>{}); });
						} else if (v.canPlayType && v.canPlayType('application/vnd.apple.mpegurl')) {
							v.src = finalSrc;
						} else {
							v.src = finalSrc;
						}
					} else {
						v.src = finalSrc;
					}
					// Set audio source if provided
					if (audioSrc && a) {
						// Check if audio also needs proxy
						try {
							const audioUrl = new URL(audioSrc);
							const currentOrigin = location.origin;
							if (audioUrl.origin !== currentOrigin && !audioSrc.startsWith('/proxy?url=')) {
								audioSrc = '/proxy?url=' + encodeURIComponent(audioSrc);
							}
						} catch (e) {
							// Use original if URL parse fails
						}
						a.src = audioSrc;
						a.style.display = '';
					}
					// Log for debugging
					console.log('Player: video src =', v.src);
					if (audioSrc) console.log('Player: audio src =', audioSrc);
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
				let mediaKind = 'direct';
				let thumbnailUrl: string | null = null;
				let videoWidth: number | null = null;
				let videoHeight: number | null = null;
				let videoDuration: number | null = null;

				// Extract video metadata from reddit_video if available
				const redditVideo = item.data?.media?.reddit_video;
				if (redditVideo) {
					videoWidth = redditVideo.width || null;
					videoHeight = redditVideo.height || null;
					videoDuration = redditVideo.duration || null;
				}

				// CRITICAL: Prefer fallback_url (full MP4 with audio+video) for Telegram inline playback
				const fallbackMp4 = redditVideo?.fallback_url as string | undefined;
				const scraperMp4 = redditVideo?.scraper_media_url as string | undefined;

				// Priority 1: fallback_url or scraper_media_url (usually complete MP4)
				if (fallbackMp4 && fallbackMp4.endsWith('.mp4')) {
					videoUrl = fallbackMp4;
					mediaKind = 'mp4_fallback';
				} else if (scraperMp4 && scraperMp4.endsWith('.mp4')) {
					videoUrl = scraperMp4;
					mediaKind = 'mp4_scraper';
				}

				// Priority 2: Check if current videoUrl is DASH (incomplete - video only)
				if (videoUrl && videoUrl.includes('DASH_') && videoUrl.endsWith('.mp4')) {
					// DASH videos are incomplete (no audio) - skip unless we already have fallback
					if (mediaKind !== 'mp4_fallback' && mediaKind !== 'mp4_scraper') {
						// Try to find the muxed version by checking for fallback_url
						if (!fallbackMp4 && !scraperMp4) {
							// No complete MP4 available, mark as DASH for potential skip
							audioUrl = videoUrl.replace(/DASH_[^/.]+\.mp4$/, 'DASH_audio.mp4');
							mediaKind = 'dash';
							log('warn:dash-only', { key, url: shortUrl(videoUrl) });
						}
					}
				}

				// NEVER use HLS for Telegram - it doesn't support .m3u8 inline playback
				// HLS is only good for web player, not for Telegram sendVideo

				// Extract thumbnail for better Telegram preview
				thumbnailUrl = extractBestPreviewImage(item.data);

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
						await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
						continue;
					}
					processedThisRun.add(rawMediaKey);
					// Reserve both keys as pending to avoid race duplicates
					await env.PROCESSED_POSTS_KV.put(key, 'pending', { expirationTtl: 604800 });
					await env.PROCESSED_POSTS_KV.put(rawMediaKey, 'pending', { expirationTtl: 604800 });
					log('reserve:pending', { key, mediaKey: rawMediaKey });
					// Log media kind only for non-skipped items
					log('media:kind', { key, kind: mediaKind, url: shortUrl(videoUrl), audio: shortUrl(audioUrl) });
				}

				log('process:start', { key, title });

				// Try to resolve video URLs:
				// 1. Non-MP4 URLs (always need resolution via Puppeteer)
				// 2. URLs that look like MP4 but might be HTML pages (check first, then resolve if HTML)
				if (videoUrl && !isDirectImageUrl(videoUrl)) {
					const isTrustedSource =
						videoUrl.includes('v.redd.it') ||
						videoUrl.includes('reddit.com') ||
						videoUrl.includes('googlevideo.com') ||
						videoUrl.includes('streamin.one');
					const isYoutube = videoUrl.includes('youtube') || videoUrl.includes('youtu.be');
					const isMp4Url = videoUrl.includes('.mp4');

					if (isMp4Url && isTrustedSource) {
						// Trusted MP4 URLs - no resolution needed
						log('resolve:trusted-mp4', { key, url: shortUrl(videoUrl) });
					} else if (isMp4Url && !isTrustedSource) {
						// MP4 URL from untrusted source - might be HTML, validate first
						log('resolve:check-mp4', { key, url: shortUrl(videoUrl) });
						const isValid = await validateVideoUrl(videoUrl);
						if (!isValid) {
							// URL looks like MP4 but returns HTML - resolve via Puppeteer
							log('resolve:mp4-is-html', { key, url: shortUrl(videoUrl), trying: 'puppeteer' });
							const resolved = await getFinalStreamUrl(env, videoUrl);
							if (resolved && resolved !== videoUrl) {
								videoUrl = resolved;
								log('resolve:done', { key, url: shortUrl(videoUrl) });
							} else {
								log('resolve:no-change', { key, url: shortUrl(videoUrl) });
							}
						} else {
							log('resolve:valid-mp4', { key, url: shortUrl(videoUrl) });
						}
					} else {
						// Non-MP4 URLs (including YouTube) - always resolve via Puppeteer
						log('resolve:open', { key, url: shortUrl(videoUrl), isYoutube });
						const resolved = await getFinalStreamUrl(env, videoUrl);
						if (resolved && resolved !== videoUrl) {
							videoUrl = resolved;
							log('resolve:done', { key, url: shortUrl(videoUrl) });
						} else {
							log('resolve:no-change', { key, url: shortUrl(videoUrl) });
						}
					}
				}

				log('media:final', { key, url: shortUrl(videoUrl), isImage: !!(videoUrl && isDirectImageUrl(videoUrl)) });

				if (videoUrl) {
					const mediaKey = `media:${canonicalizeUrl(videoUrl)}`;
					// Only re-check dedupe if final mediaKey differs from early reserved rawMediaKey
					if (!rawMediaKey || mediaKey !== rawMediaKey) {
						if (processedThisRun.has(mediaKey) || (await env.PROCESSED_POSTS_KV.get(mediaKey))) {
							log('skip:kv-media-final', { key, mediaKey });
							await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
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
				Извлеки хештеги из строки.
		Только хештеги, через пробел. Никаких комментариев и кода.

		Правила генерации:
		- Используй только названия команд и игроков.
		- ВАЖНО: в теле тега допускаются ТОЛЬКО английские буквы (A-Z, a-z) и цифры (0-9). Диакритики замени: ä→a, ö→o, å→a, é→e, ñ→n и т.п. Прочие символы удали.
		- В конце добавь ОДИН тег события из списка: #Goal #Assist #Save #RedCard #YellowCard #Penalty #PenaltyMiss #OwnGoal #Injury #Transfer #Interview #Statistics.
		- Запрещено ставить #Goal, если в строке нет явных признаков гола (счёт вида "x - y", минуты "45'", глаголов score/scored/goal/GOL и т.п.). 
		- Для цитат/интервью/пресс-конференций выбирай #Interview.
		- Для таблиц, статистики, графиков, чисел, данных, рейтингов выбирай #Statistics.
		- Подсказки: слова вроде "says", "quote", кавычки с речью игрока/тренера, метки IG/Instagram обычно означают интервью/цитату → #Interview.
		- Подсказки: слова "table", "stats", "statistics", "top", "ranking", "data", "numbers", таблицы с данными → #Statistics.
		- Порядок тегов: команды → игроки → событие.
		- Если команд/игроков нет — выбери 1–3 ключевых тега по смыслу + событие.

		Примеры:
		Annecy 1-0 Caen - Yohann Demoncy 13'
		#Annecy #Caen #YohannDemoncy #Goal

		Bernardo Silva: "We must improve before derby"
		#BernardoSilva #Interview

		Hammarby vs Häcken: Ultra Boys protest...
		#Hammarby #Hacken #UltraBoys #Interview

		Top 10 goalscorers in La Liga this season
		#LaLiga #Statistics

		Теперь ответь для моей строки:
`;

					try {
						const safeTitle = escapeHtml(decodeHtmlEntities(title));

						// Generate hashtags first
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
						}

						// Build message: title (with source link and player link) + hashtags on new line
						let titleLine = safeTitle;
						// Always add source link and player link for video content
						if (videoUrl) {
							titleLine += ` <a href="${escapeHtml(videoUrl)}">↗</a>`;
							// Add player link for MP4 and M3U8 videos
							if (videoUrl.includes('.mp4') || videoUrl.includes('.m3u8')) {
								const playerUrl = `/player?video=${encodeURIComponent(videoUrl)}${
									audioUrl ? `&audio=${encodeURIComponent(audioUrl)}` : ''
								}`;
								const absPlayerUrl = new URL(playerUrl, 'https://golissimo.miroshnik.workers.dev').toString();
								titleLine += ` <a href="${escapeHtml(absPlayerUrl)}">▷</a>`;
							}
						}

						let message = titleLine;
						if (aiText) {
							message += `\n${aiText}`;
						}

						// Check if this is an image
						const isImage = videoUrl && isDirectImageUrl(videoUrl);

						// Handle images with sendPhoto
						if (isImage) {
							log('tg:send', { key, method: 'sendPhoto', photo: shortUrl(videoUrl) });
							try {
								const response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendPhoto`, {
									method: 'POST',
									headers: { 'Content-Type': 'application/json' },
									body: JSON.stringify({
										chat_id: env.TELEGRAM_CHAT_ID,
										photo: videoUrl,
										caption: message,
										parse_mode: 'HTML',
									}),
								});

								if (!response.ok) {
									let bodyText = '';
									try {
										bodyText = await response.text();
									} catch {}
									console.error('tg:error', { status: response.status, statusText: response.statusText, body: bodyText.slice(0, 500) });
									await env.PROCESSED_POSTS_KV.delete(mediaKey);
									await env.PROCESSED_POSTS_KV.delete(key);
									continue;
								}

								log('tg:ok', { key, type: 'photo' });
								await env.PROCESSED_POSTS_KV.put(key, '0', { expirationTtl: 604800 });
								await env.PROCESSED_POSTS_KV.put(mediaKey, '1', { expirationTtl: 604800 });
								continue; // Skip video processing
							} catch (e) {
								console.error('pipeline:error', e);
								await env.PROCESSED_POSTS_KV.delete(mediaKey);
								await env.PROCESSED_POSTS_KV.delete(key);
								continue;
							}
						}

						const isMp4 = videoUrl.endsWith('.mp4');
						const isYoutubeVideo = videoUrl.includes('googlevideo.com/videoplayback');
						const isVideoUrl = isMp4 || isYoutubeVideo;
						const isDash = videoUrl.includes('DASH_');
						let response: Response;

						// Validate that URL actually points to a video file
						let isValidVideo = false;
						if (isVideoUrl) {
							// Quick check: Reddit, Streamain, and Streamin CDNs are trusted without validation
							const isRedditVideo = videoUrl.includes('v.redd.it') || videoUrl.includes('reddit.com');
							const isStreamainVideo = videoUrl.includes('streamain.com');
							const isStreaminVideo = videoUrl.includes('streamin.one');
							const isTrustedDomain = isRedditVideo || isStreamainVideo || isStreaminVideo;

							if (isTrustedDomain) {
								log('validate:trusted', {
									url: shortUrl(videoUrl),
									reddit: isRedditVideo,
									streamain: isStreamainVideo,
									streamin: isStreaminVideo,
								});
								isValidVideo = true;
							} else {
								// YouTube and other URLs must be validated (they may expire, require auth, etc.)
								log('validate:checking', { url: shortUrl(videoUrl), isYoutube: isYoutubeVideo });
								isValidVideo = await validateVideoUrl(videoUrl);
							}
						}

						if (!isValidVideo && isVideoUrl) {
							// URL looks like video but isn't actually valid (HTML page, redirect, etc.)
							if (retriesLeft > 1) {
								log('skip:invalid-video-url', { key, retriesLeft, url: shortUrl(videoUrl) });
								await env.PROCESSED_POSTS_KV.put(key, (retriesLeft - 1).toString(), { expirationTtl: 604800 });
								await env.PROCESSED_POSTS_KV.delete(mediaKey);
								continue;
							}
							// Last retry: this is HTML, try to resolve via puppeteer to find real video
							log('retry:resolve-html', { key, url: shortUrl(videoUrl) });
							const resolvedFromHtml = await getFinalStreamUrl(env, videoUrl);
							if (resolvedFromHtml && resolvedFromHtml !== videoUrl) {
								log('retry:found-new', { key, old: shortUrl(videoUrl), new: shortUrl(resolvedFromHtml) });
								videoUrl = resolvedFromHtml;
								// Update checks for new URL
								const newIsMp4 = videoUrl.endsWith('.mp4');
								const newIsYoutube = videoUrl.includes('googlevideo.com/videoplayback');
								const newIsDash = videoUrl.includes('DASH_');
								// Re-validate the new URL (Reddit, Streamain, and Streamin are trusted)
								const isRedditVideo = videoUrl.includes('v.redd.it') || videoUrl.includes('reddit.com');
								const isStreamainVideo = videoUrl.includes('streamain.com');
								const isStreaminVideo = videoUrl.includes('streamin.one');
								if (isRedditVideo || isStreamainVideo || isStreaminVideo) {
									isValidVideo = true;
								} else if (newIsMp4 || newIsYoutube) {
									// YouTube and other URLs must be validated (they often expire or need auth)
									isValidVideo = await validateVideoUrl(videoUrl);
								}
								log('retry:revalidated', {
									key,
									url: shortUrl(videoUrl),
									isValid: isValidVideo,
									isMp4: newIsMp4,
									isYoutube: newIsYoutube,
									isDash: newIsDash,
								});
							} else {
								log('retry:no-new-url', { key, url: shortUrl(videoUrl) });
							}
						}

						// Try to send as video if it's a valid video URL
						// DASH videos (video-only) are skipped on early attempts, but sent on last attempt
						// Re-check video URL type and isDash in case URL changed
						const finalIsMp4 = videoUrl.endsWith('.mp4');
						const finalIsYoutube = videoUrl.includes('googlevideo.com/videoplayback');
						const finalIsVideoUrl = finalIsMp4 || finalIsYoutube;
						const finalIsDash = videoUrl.includes('DASH_');
						let shouldSendVideo = finalIsVideoUrl && isValidVideo && (!finalIsDash || retriesLeft <= 1);

						// Check if source is trusted before sending
						const isRedditVideo = videoUrl.includes('v.redd.it') || videoUrl.includes('reddit.com');
						const isYouTubeVideo = videoUrl.includes('googlevideo.com');
						const isStreamainCDN = videoUrl.includes('streamain.com'); // Works well with Telegram
						const isStreaminCDN = videoUrl.includes('streamin.one'); // Works well with Telegram
						const isTrustedSource = isRedditVideo || isYouTubeVideo || isStreamainCDN || isStreaminCDN;

						// For non-trusted sources, retry instead of sending (they often fail in Telegram)
						// But on last attempt (retriesLeft <= 1), send as text with player link
						if (shouldSendVideo && !isTrustedSource && retriesLeft > 1) {
							log('skip:untrusted-source', { key, retriesLeft, url: shortUrl(videoUrl), reason: 'external-url-retry' });
							await env.PROCESSED_POSTS_KV.put(key, (retriesLeft - 1).toString(), { expirationTtl: 604800 });
							await env.PROCESSED_POSTS_KV.delete(mediaKey);
							continue;
						}

						// On last attempt with untrusted source, send as text (will be handled in else branch)
						if (shouldSendVideo && !isTrustedSource && retriesLeft <= 1) {
							log('untrusted:last-attempt', { key, url: shortUrl(videoUrl), reason: 'send-as-text' });
							// Don't send as video, fall through to text message
							shouldSendVideo = false;
						}

						if (shouldSendVideo && isTrustedSource) {
							const logMsg = finalIsDash ? 'sendVideo-dash-noaudio' : 'sendVideo';
							log('tg:send', {
								key,
								method: logMsg,
								video: shortUrl(videoUrl),
								trusted: true,
								source: isRedditVideo
									? 'reddit'
									: isStreamainCDN
									? 'streamain'
									: isStreaminCDN
									? 'streamin'
									: isYouTubeVideo
									? 'youtube'
									: 'other',
								thumb: shortUrl(thumbnailUrl),
							});

							let caption = message;
							// Warn user if sending DASH (no audio)
							if (finalIsDash) {
								caption += '\n⚠️ no audio';
							}

							const videoPayload: any = {
								chat_id: env.TELEGRAM_CHAT_ID,
								video: videoUrl,
								caption: caption,
								parse_mode: 'HTML',
								supports_streaming: true,
							};
							// Add metadata for better preview generation
							if (thumbnailUrl) videoPayload.thumbnail = thumbnailUrl;
							if (videoWidth) videoPayload.width = videoWidth;
							if (videoHeight) videoPayload.height = videoHeight;
							if (videoDuration) videoPayload.duration = Math.floor(videoDuration);
							response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendVideo`, {
								method: 'POST',
								headers: { 'Content-Type': 'application/json' },
								body: JSON.stringify(videoPayload),
							});
						} else {
							// Not valid video or should retry to find better version
							if (retriesLeft > 1) {
								const reason = !isValidVideo ? 'invalid-url' : finalIsDash ? 'dash' : !isTrustedSource ? 'untrusted' : 'no-mp4';
								log('skip:incomplete-video', { key, retriesLeft, reason, url: shortUrl(videoUrl) });
								await env.PROCESSED_POSTS_KV.put(key, (retriesLeft - 1).toString(), { expirationTtl: 604800 });
								await env.PROCESSED_POSTS_KV.delete(mediaKey);
								continue;
							}
							// Last attempt: no valid video or untrusted source, send as text message with link
							// Links already added to titleLine above (↗ source and ▷ player)
							const textWithLink = aiText ? `${titleLine}\n${aiText}` : titleLine;
							const reason = !isValidVideo
								? 'invalid-url'
								: finalIsDash
								? 'dash-skipped'
								: !isTrustedSource
								? 'untrusted-source'
								: 'no-mp4';
							log('tg:send', { key, method: 'sendMessage-last', reason });
							response = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
								method: 'POST',
								headers: { 'Content-Type': 'application/json' },
								body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text: textWithLink, parse_mode: 'HTML' }),
							});
						}

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
	// 1) Native Reddit video - try different quality options
	const redditVideo = d?.media?.reddit_video;
	if (redditVideo) {
		// Try fallback_url first (usually 720p or 480p)
		if (redditVideo.fallback_url) return String(redditVideo.fallback_url);
		// Try scraper_media_url as alternative
		if (redditVideo.scraper_media_url) return String(redditVideo.scraper_media_url);
		// HLS url (muxed audio+video)
		if (redditVideo.hls_url) return String(redditVideo.hls_url);
	}

	// 2) Preview variants - prefer MP4 over GIF for better quality
	const pimg = d?.preview?.images?.[0];
	if (pimg?.variants) {
		// Try mp4 variant first (usually better quality than gif)
		const mp4 = pimg.variants?.mp4?.source?.url;
		if (mp4) return decodeRedditUrl(String(mp4));
		const gif = pimg.variants?.gif?.source?.url;
		if (gif) return decodeRedditUrl(String(gif));
	}

	// 3) Gallery posts (multiple images)
	if (d?.is_gallery && d?.gallery_data?.items && d?.media_metadata) {
		for (const it of d.gallery_data.items as any[]) {
			const meta = d.media_metadata?.[it.media_id];
			if (!meta) continue;
			// Prefer original (s), fallback to largest preview (p)
			const src = meta.s?.u || meta.s?.gif || (Array.isArray(meta.p) && meta.p.length ? meta.p[meta.p.length - 1]?.u : null);
			if (src) return decodeRedditUrl(String(src));
		}
	}

	// 4) Preview image source
	if (pimg?.source?.url) return decodeRedditUrl(String(pimg.source.url));

	// 5) Overridden URL or direct media link
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

const decodeHtmlEntities = (s: string): string => {
	return s
		.replace(/&amp;/g, '&')
		.replace(/&lt;/g, '<')
		.replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"')
		.replace(/&#39;/g, "'");
};

const getFinalStreamUrl = async (env: Env, streamUrl: string): Promise<string | null> => {
	const browser = await puppeteer.launch(env.BROWSER);
	const page = await browser.newPage();

	await page.setUserAgent(
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
	);

	let finalStreamUrl: string | null = null;
	let foundEarly = false;

	try {
		await page.setRequestInterception(true);

		page.on('request', async (request) => {
			try {
				const url = request.url();
				if (!finalStreamUrl && isMp4Url(url)) {
					log('resolve:found-request', { url: shortUrl(url) });
					finalStreamUrl = url;
					foundEarly = true;
				}
				await request.continue().catch(() => {}); // Ignore if already handled
			} catch (e) {
				// Ignore errors in request handler
			}
		});

		page.on('response', async (response) => {
			try {
				const url = response.url();
				if (!finalStreamUrl && isMp4Url(url)) {
					log('resolve:found-response', { url: shortUrl(url) });
					finalStreamUrl = url;
					foundEarly = true;
				}
			} catch (e) {
				// Ignore errors in response handler
			}
		});

		try {
			// Use very short timeout - we only need to capture network requests
			await page.goto(streamUrl, { waitUntil: 'domcontentloaded', timeout: 10000 });

			// If we found video URL during page load, return immediately
			if (foundEarly && finalStreamUrl) {
				log('resolve:early-return', { url: shortUrl(finalStreamUrl) });
				return finalStreamUrl;
			}
		} catch (e) {
			// If page load fails but we found URL, still return it
			if (finalStreamUrl) {
				log('resolve:error-but-found', { url: shortUrl(finalStreamUrl) });
				return finalStreamUrl;
			}
			// WebSocket errors are common - log but don't spam
			const errStr = String(e);
			if (!errStr.includes('Websocket') && !errStr.includes('WebSocket')) {
				log('resolve:goto-error', { error: errStr.slice(0, 100) });
			}
		}

		// Only continue searching if not found yet
		if (!finalStreamUrl) {
			try {
				const req = await page.waitForRequest((req) => isMp4Url(req.url()), { timeout: 5000 });
				finalStreamUrl = req.url();
				log('resolve:found-wait', { url: shortUrl(finalStreamUrl) });
			} catch (_) {
				// ignore timeout - just move on
			}
		}

		// Try DOM scraping for <video>/<source> src attributes
		if (!finalStreamUrl) {
			try {
				const domUrls: string[] = await page.evaluate(() => {
					const urls = new Set<string>();
					const list: any[] = Array.from((globalThis as any).document?.querySelectorAll?.('video,source') || []);
					for (const el of list) {
						const src: string = el?.src || el?.getAttribute?.('src') || '';
						if (src) urls.add(src);
					}
					return Array.from(urls);
				});
				for (const u of domUrls) {
					if (isMp4Url(u)) {
						finalStreamUrl = u;
						log('resolve:found-dom', { url: shortUrl(finalStreamUrl) });
						break;
					}
				}
			} catch (e) {
				log('resolve:dom-error', { error: String(e).slice(0, 50) });
			}
		}

		// As a last resort, scan performance entries
		if (!finalStreamUrl) {
			try {
				const perfUrls: string[] = await page.evaluate(() => {
					const perf = (globalThis as any).performance;
					const entries = perf && perf.getEntriesByType ? perf.getEntriesByType('resource') : [];
					return (entries as any[]).map((e) => String(e.name || ''));
				});
				for (const u of perfUrls) {
					if (isMp4Url(u)) {
						finalStreamUrl = u;
						log('resolve:found-perf', { url: shortUrl(finalStreamUrl) });
						break;
					}
				}
			} catch (e) {
				log('resolve:perf-error', { error: String(e).slice(0, 50) });
			}
		}

		log('resolve:return', { url: shortUrl(finalStreamUrl), found: !!finalStreamUrl });
		return finalStreamUrl;
	} catch (error) {
		log('resolve:error', { src: shortUrl(streamUrl), error: String(error).slice(0, 100), hadUrl: !!finalStreamUrl });
		// Return whatever we found even if error occurred
		return finalStreamUrl;
	} finally {
		// Always close browser to free resources - do it aggressively
		try {
			// Set a timeout for closing to avoid hanging
			await Promise.race([browser.close(), new Promise((_, reject) => setTimeout(() => reject(new Error('Close timeout')), 3000))]);
		} catch (e) {
			// Suppress WebSocket errors during close - they're expected
			const errStr = String(e);
			if (!errStr.includes('Websocket') && !errStr.includes('WebSocket') && !errStr.includes('Close timeout')) {
				log('resolve:close-error', { error: errStr.slice(0, 50) });
			}
		}
	}
};

const isMp4Url = (url: string): boolean => {
	// Check for direct .mp4 URLs (excluding low quality DASH_96)
	if (url.includes('.mp4') && !url.includes('DASH_96.')) {
		return true;
	}
	// Check for YouTube videoplayback URLs (googlevideo.com/videoplayback)
	if (url.includes('googlevideo.com/videoplayback')) {
		return true;
	}
	// Check for other common video CDN patterns
	if (url.includes('/video') && (url.includes('mime=video') || url.includes('contenttype=video'))) {
		return true;
	}
	return false;
};

const validateVideoUrl = async (url: string): Promise<boolean> => {
	try {
		// HEAD request to check Content-Type without downloading full file
		// Use Telegram-like User-Agent to match what Telegram will see
		const response = await fetch(url, {
			method: 'HEAD',
			headers: {
				'User-Agent': 'TelegramBot (like TwitterBot)',
				Accept: 'video/*,application/octet-stream',
			},
			redirect: 'follow',
		});

		if (!response.ok) {
			log('validate:http-error', { url: shortUrl(url), status: response.status });
			return false;
		}

		const contentType = (response.headers.get('content-type') || '').toLowerCase();
		const contentLength = response.headers.get('content-length');

		// Reject HTML pages explicitly
		if (contentType.includes('text/html') || contentType.includes('text/plain')) {
			log('validate:html-page', { url: shortUrl(url), contentType });
			return false;
		}

		// Don't trust Content-Type alone - some servers return video/mp4 on HEAD but HTML on GET
		// Download first 1KB to verify it's really a video
		if (contentType.includes('video/') || contentType.includes('application/octet-stream')) {
			try {
				const getResponse = await fetch(url, {
					method: 'GET',
					headers: {
						'User-Agent': 'TelegramBot (like TwitterBot)',
						Accept: 'video/*,application/octet-stream',
						Range: 'bytes=0-1023', // First 1KB
					},
					redirect: 'follow',
				});

				if (!getResponse.ok && getResponse.status !== 206) {
					log('validate:get-failed', { url: shortUrl(url), status: getResponse.status });
					return false;
				}

				const actualContentType = (getResponse.headers.get('content-type') || '').toLowerCase();

				// Check if actual response is HTML
				if (actualContentType.includes('text/html') || actualContentType.includes('text/plain')) {
					log('validate:get-html', { url: shortUrl(url), actualContentType });
					return false;
				}

				// Read first bytes to check if it looks like HTML
				const chunk = await getResponse.text();
				if (chunk.trim().startsWith('<') || chunk.includes('<!DOCTYPE') || chunk.includes('<html')) {
					log('validate:body-html', { url: shortUrl(url), preview: chunk.slice(0, 50) });
					return false;
				}

				// Check size for octet-stream
				if (contentType.includes('application/octet-stream')) {
					const size = contentLength ? parseInt(contentLength, 10) : 0;
					if (size < 100000) {
						log('validate:too-small', { url: shortUrl(url), size });
						return false;
					}
				}

				// Special warning for YouTube URLs (they may work now but expire soon)
				const isYouTubeUrl = url.includes('googlevideo.com');
				if (isYouTubeUrl) {
					log('validate:youtube-warning', { url: shortUrl(url), note: 'URL may expire' });
				}

				log('validate:ok-video', { url: shortUrl(url), contentType, actualContentType, size: contentLength, isYouTube: isYouTubeUrl });
				return true;
			} catch (rangeError) {
				log('validate:range-failed', { url: shortUrl(url), error: String(rangeError).slice(0, 50) });
				return false;
			}
		}

		// Reject if Content-Type is missing or unknown
		log('validate:unknown-type', { url: shortUrl(url), contentType });
		return false;
	} catch (error) {
		log('validate:network-error', { url: shortUrl(url), error: String(error).slice(0, 100) });
		// IMPORTANT: Return false on network errors - don't assume URL is valid
		return false;
	}
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

const extractBestPreviewImage = (d: any): string | null => {
	// 1) media_metadata (gallery)
	if (d?.is_gallery && d?.gallery_data?.items && d?.media_metadata) {
		for (const it of d.gallery_data.items as any[]) {
			const meta = d.media_metadata?.[it.media_id];
			if (!meta) continue;
			const src = meta.s?.u || (Array.isArray(meta.p) && meta.p.length ? meta.p[meta.p.length - 1]?.u : null);
			if (src) return decodeRedditUrl(String(src));
		}
	}
	// 2) preview.images[0]
	const pimg = d?.preview?.images?.[0];
	if (pimg) {
		const src =
			pimg.source?.url ||
			(Array.isArray(pimg.resolutions) && pimg.resolutions.length ? pimg.resolutions[pimg.resolutions.length - 1]?.url : null);
		if (src) return decodeRedditUrl(String(src));
	}
	// 3) direct overridden url if it's image
	const overridden = d?.url_overridden_by_dest;
	if (overridden && /\.(jpe?g|png|webp|gif)(\?|$)/i.test(overridden)) return overridden;
	return null;
};

const fetchOgImageSafe = async (url: string): Promise<string | null> => {
	try {
		const resp = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
		if (!resp.ok) return null;
		const html = await resp.text();
		const m =
			html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) ||
			html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i);
		return m ? m[1] : null;
	} catch {
		return null;
	}
};
