import { spawn, execSync } from 'child_process';
import puppeteer from 'puppeteer';
import dotenv from 'dotenv';

dotenv.config();

// Check if FFmpeg is installed
function checkFFmpeg(): void {
	try {
		execSync('ffmpeg -version', { stdio: 'ignore' });
		console.log('✓ FFmpeg found');
	} catch {
		console.error('✗ FFmpeg not found. Install it first:');
		console.error('  macOS: brew install ffmpeg');
		console.error('  Ubuntu/Debian: sudo apt install ffmpeg');
		console.error('  Windows: download from https://ffmpeg.org/download.html');
		process.exit(1);
	}
}

// Get stream URL from any player page using Puppeteer
async function getStreamUrl(playerUrl: string): Promise<{ url: string; cookies: string } | null> {
	console.log('🔍 Opening player page...');
	console.log('URL:', playerUrl, '\n');

	const browser = await puppeteer.launch({
		headless: true, // Run in background
		args: ['--no-sandbox', '--disable-setuid-sandbox'],
	});

	const foundUrls: string[] = [];
	let cookieString = '';

	try {
		const page = await browser.newPage();

		// Intercept network requests to find M3U8
		await page.setRequestInterception(true);
		page.on('request', (request) => {
			request.continue();
		});

		page.on('response', async (response) => {
			const url = response.url();

			if (url.includes('.m3u8')) {
				console.log('✓ Found M3U8:', url);
				foundUrls.push(url);
			}

			if (url.includes('.ts') && !url.includes('.m3u8') && !url.includes('ads') && !url.includes('.cts')) {
				const m3u8Url = url.substring(0, url.lastIndexOf('/')) + '/index.m3u8';
				if (!foundUrls.includes(m3u8Url)) {
					foundUrls.push(m3u8Url);
				}
			}

			if (url.includes('.mp4') && !url.includes('ads')) {
				console.log('✓ Found MP4:', url);
				foundUrls.push(url);
			}
		});

		await page.setUserAgent(
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
		);

		console.log('Loading player page...');
		await page.goto(playerUrl, {
			waitUntil: 'domcontentloaded',
			timeout: 60000,
		});

		// Wait for page to load
		console.log('Waiting for player to load...');
		await new Promise((resolve) => setTimeout(resolve, 5000));

		// Multiple click attempts to start video playback
		console.log('Clicking to start video...');
		for (let i = 0; i < 3; i++) {
			try {
				await page.mouse.click(640, 360);
				await new Promise((resolve) => setTimeout(resolve, 2000));
			} catch (e) {
				// Ignore
			}
		}

		// Wait for stream to load
		console.log('Monitoring network for stream URLs (20 seconds)...\n');
		await new Promise((resolve) => setTimeout(resolve, 20000));

		// Filter out blob URLs
		const validUrls = foundUrls.filter((u) => !u.startsWith('blob:'));

		if (validUrls.length > 0) {
			const m3u8Url = validUrls.find((u) => u.includes('.m3u8') && !u.includes('/url_'));
			const streamUrl = m3u8Url || validUrls[0];

			console.log(`✅ Found stream URL: ${streamUrl}`);
			if (validUrls.length > 1) {
				console.log(`   (${validUrls.length} total URLs found)`);
			}

			// Get cookies
			const cookies = await page.cookies();
			cookieString = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
			console.log(`✅ Captured ${cookies.length} cookies from browser\n`);

			// Close browser
			await browser.close();

			return { url: streamUrl, cookies: cookieString };
		} else {
			console.log('✗ No valid stream URL found');
			if (foundUrls.length > 0) {
				console.log(`  Found ${foundUrls.length} blob: URLs, but they cannot be used with FFmpeg`);
			}
			await browser.close();
			return null;
		}
	} catch (error) {
		console.error('Error extracting stream URL:', error);
		await browser.close();
		return null;
	}

	// NOTE: Browser stays open to keep the stream token valid!
}

// Start FFmpeg stream to Telegram
function startStream(streamUrl: string, referer?: string, cookies?: string): void {
	let rtmpsUrl = process.env.TELEGRAM_RTMPS_URL;
	const streamKey = process.env.TELEGRAM_STREAM_KEY;

	if (!rtmpsUrl || !streamKey) {
		console.error('Error: TELEGRAM_RTMPS_URL or TELEGRAM_STREAM_KEY not set in .env');
		process.exit(1);
	}

	// Try RTMP without SSL (more stable)
	if (rtmpsUrl.startsWith('rtmps://')) {
		console.log('⚠️  Converting RTMPS to RTMP for better compatibility');
		rtmpsUrl = rtmpsUrl.replace('rtmps://', 'rtmp://');
	}

	const fullRtmpsUrl = `${rtmpsUrl}${streamKey}`;

	console.log('🔴 Starting stream...');
	console.log('Source:', streamUrl);
	console.log('Target:', fullRtmpsUrl.replace(streamKey, '***KEY***'));
	if (referer) {
		console.log('Using Referer:', referer);
	}
	if (cookies) {
		console.log('Using Cookies:', cookies.length > 50 ? cookies.substring(0, 50) + '...' : cookies);
	}
	console.log('\nPress Ctrl+C to stop\n');

	let ffmpegArgs: string[] = ['-re'];

	// Add HTTP headers
	if (referer) {
		const origin = new URL(referer).origin;
		let headers = `Referer: ${referer}\r\nOrigin: ${origin}\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n`;

		if (cookies) {
			headers += `Cookie: ${cookies}\r\n`;
		}

		ffmpegArgs.push('-headers', headers);
	} else {
		ffmpegArgs.push(
			'-user_agent',
			'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
		);
	}

	ffmpegArgs.push(
		// Read at native frame rate (prevents rushing) - MUST be before -i
		'-re',
		// Input buffering
		'-rtbufsize',
		'100M',
		'-probesize',
		'10M',
		'-analyzeduration',
		'10M',
		'-i',
		streamUrl,
		// Copy video stream without re-encoding (MUCH faster!)
		'-c:v',
		'copy',
		// Re-encode audio to AAC (lightweight)
		'-c:a',
		'aac',
		'-b:a',
		'128k',
		'-ac',
		'2',
		'-ar',
		'44100',
		// Output buffering for stability
		'-bufsize',
		'3000k',
		'-maxrate',
		'3000k',
		// Output format
		'-f',
		'flv',
		fullRtmpsUrl
	);

	const ffmpeg = spawn('ffmpeg', ffmpegArgs);

	ffmpeg.stdout.on('data', (data) => {
		process.stdout.write(data);
	});

	ffmpeg.stderr.on('data', (data) => {
		process.stderr.write(data);
	});

	ffmpeg.on('close', (code) => {
		console.log(`\nFFmpeg process exited with code ${code}`);
		process.exit(code || 0);
	});

	ffmpeg.on('error', (err) => {
		console.error('FFmpeg error:', err);
		process.exit(1);
	});

	// Handle Ctrl+C
	process.on('SIGINT', () => {
		console.log('\n\nStopping stream...');
		ffmpeg.kill('SIGTERM');
		setTimeout(() => {
			ffmpeg.kill('SIGKILL');
			process.exit(0);
		}, 5000);
	});
}

// Main CLI
async function main() {
	const command = process.argv[2];

	if (!command || command === 'help') {
		console.log('Usage:');
		console.log('  npm run stream:start <player_url>');
		console.log('');
		console.log('Example:');
		console.log('  npm run stream:start https://cdn.livetv868.me/webplayer.php?t=ifr&c=2847751&lang=ru');
		console.log('  npm run stream:start https://gamestrend.net/match/12345');
		process.exit(0);
	}

	if (command === 'start') {
		checkFFmpeg();

		const playerUrl = process.argv[3];

		if (!playerUrl) {
			console.error('❌ Error: Please provide a player page URL');
			console.error('');
			console.error('Usage: npm run stream:start <player_url>');
			console.error('');
			console.error('Example:');
			console.error('  npm run stream:start https://cdn.livetv868.me/webplayer.php?...');
			process.exit(1);
		}

		// Validate URL
		if (!playerUrl.startsWith('http://') && !playerUrl.startsWith('https://')) {
			console.error('❌ Error: URL must start with http:// or https://');
			process.exit(1);
		}

		try {
			// Get stream URL from player page
			const streamData = await getStreamUrl(playerUrl);

			if (!streamData) {
				console.error('❌ Could not find stream URL on this page');
				console.error('Make sure the page has a video player');
				process.exit(1);
			}

			// Start FFmpeg with referer and cookies
			startStream(streamData.url, playerUrl, streamData.cookies);
		} catch (error) {
			console.error('❌ Error starting stream:', error);
			process.exit(1);
		}
	} else {
		console.error(`❌ Unknown command: ${command}`);
		console.log('Usage: npm run stream:start <player_url>');
		process.exit(1);
	}
}

main().catch((error) => {
	console.error('Fatal error:', error);
	process.exit(1);
});
