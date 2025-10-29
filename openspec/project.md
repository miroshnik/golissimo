# Project Context

## Purpose

**Golissimo** is a Cloudflare Worker that automatically monitors r/soccer subreddit for media posts (videos and images), extracts media URLs, generates relevant hashtags using AI, and posts curated content to a Telegram channel. The system processes Reddit posts with "Media" flair, handles various video formats (MP4, HLS, DASH), resolves URLs when needed using browser automation, and ensures deduplication to avoid reposting content.

Key goals:

- Automatically aggregate soccer media from Reddit
- Extract and resolve direct video/image URLs
- Generate contextual hashtags using AI (teams, players, events)
- Post to Telegram with media previews
- Prevent duplicate content via KV-based deduplication

## Tech Stack

- **TypeScript** (ES2021 target, strict mode enabled)
- **Cloudflare Workers** (serverless runtime)
- **Cloudflare Workers AI** (@cf/meta/llama-3-8b-instruct for hashtag generation)
- **Cloudflare Puppeteer** (browser automation for URL resolution)
- **Cloudflare KV** (deduplication and state storage)
- **Vitest** with `@cloudflare/vitest-pool-workers` (testing)
- **Wrangler** (deployment and development)

## Project Conventions

### Code Style

- **Indentation**: Tabs (not spaces)
- **TypeScript**: Strict mode enabled, ES2021 target
- **Logging**: logfmt-style structured logging (`evt=event_name key=value key2=value2`)
- **Naming**:
  - Functions: camelCase (`fetchRedditNew`, `extractBestMediaUrl`)
  - Variables: camelCase
  - Constants: camelCase (no SCREAMING_CASE)
- **File structure**: Single main file (`src/index.ts`) with helper functions, separate utilities in `src/stream.ts`
- **No classes**: Function-based architecture (no OOP patterns)
- **Error handling**: Try-catch blocks with logging, graceful degradation
- **URL handling**: Always use `shortUrl()` helper for logging to avoid redaction

### Architecture Patterns

- **Cron-based processing**: Scheduled worker runs every minute (`* * * * *`)
- **KV deduplication**: Two-level deduplication:
  - `post:*` keys for Reddit posts (prevents processing same post multiple times)
  - `media:*` keys for canonicalized media URLs (prevents reposting same media)
- **Retry logic**: Posts get 5 retry attempts stored in KV as retry count
- **Media resolution pipeline**:
  1. Extract media URL from Reddit post metadata
  2. Validate/resolve URLs (trusted sources vs. requiring Puppeteer)
  3. Validate video URLs before sending to Telegram
  4. Handle different media types (images, MP4, DASH, HLS)
- **Request handlers**:
  - `GET /`: Lists processed posts from KV
  - `GET /player`: HTML5 video player page with HLS.js support
  - `POST /admin/kv/clear`: Admin endpoint to clear KV namespace (requires auth token)
- **Trusted sources**: Reddit (v.redd.it, reddit.com), Google Video (googlevideo.com), Streamain CDN - these don't require URL validation
- **Early reservation**: Reserve both post and media keys as "pending" before heavy processing to prevent race conditions

### Testing Strategy

- **Framework**: Vitest with Cloudflare Workers pool
- **Config**: `vitest.config.mts` using `@cloudflare/vitest-pool-workers`
- **Test files**: Not yet created, but should mirror `src/` structure in a `test/` directory
- **Type checking**: TypeScript compiler handles type checking (no separate lint step)

### Git Workflow

- Branching strategy: Not specified (defaults to main branch)
- Commit conventions: Not specified

## Domain Context

- **Soccer/Football media aggregation**: Focus on r/soccer subreddit posts with "Media" flair
- **Hashtag generation**: Uses Russian-language prompts to extract:
  - Team names (normalized to ASCII: ä→a, ö→o, etc.)
  - Player names (same normalization)
  - Event types: #Goal, #Assist, #Save, #RedCard, #YellowCard, #Penalty, #PenaltyMiss, #OwnGoal, #Injury, #Transfer, #Interview, #Statistics
- **Media formats**:
  - **MP4**: Preferred format for Telegram inline playback
  - **DASH**: Reddit's segmented video format (video-only, separate audio track)
  - **HLS (.m3u8)**: Supported in web player but NOT for Telegram sendVideo (only for `/player` page)
  - **Images**: JPG, PNG, GIF, WebP - sent via `sendPhoto`
- **URL resolution**: Many URLs are HTML pages that load video players - Puppeteer intercepts network requests to find actual `.mp4` URLs
- **Telegram integration**:
  - Video posts use `sendVideo` with thumbnail, dimensions, duration
  - Image posts use `sendPhoto`
  - Fallback to `sendMessage` with links if video URL invalid/untrusted
  - Supports streaming with `supports_streaming: true`

## Important Constraints

- **Cloudflare Workers limits**:
  - CPU time limits per request
  - Puppeteer browser sessions must be closed aggressively (3s timeout)
  - KV operations have rate limits
- **Telegram API limits**:
  - File size limits for videos
  - Rate limits on messages
- **Reddit API**: 429 rate limiting handled gracefully
- **Media URL validation**: Must validate URLs before sending to Telegram (many URLs are HTML pages, not actual video files)
- **DASH videos**: Reddit's DASH format has no audio - system attempts to find fallback MP4 with audio, or skips on early retries
- **Untrusted sources**: External video URLs are retried before sending (may expire or fail in Telegram)
- **HLS not supported by Telegram**: `.m3u8` URLs only work in web player, not Telegram inline playback
- **Browser automation**: Expensive operation, only used when necessary (non-MP4 URLs, untrusted sources)

## External Dependencies

- **Reddit API**: `https://api.reddit.com/r/soccer/new?limit=25` - Fetches new posts
- **Telegram Bot API**: `https://api.telegram.org/bot{token}/sendVideo`, `sendPhoto`, `sendMessage`
- **Media CDNs**:
  - `v.redd.it` - Reddit's video CDN (trusted)
  - `preview.redd.it` - Reddit's image CDN
  - `googlevideo.com` - Google Video CDN (trusted, may expire)
  - `streamain.com` - Third-party video CDN (trusted)
- **HLS.js**: CDN-hosted library (`https://cdn.jsdelivr.net/npm/hls.js@1.5.7/dist/hls.min.js`) for HLS playback in web player
- **Cloudflare Services** (bindings in `wrangler.json`):
  - KV Namespace: `PROCESSED_POSTS_KV` - Deduplication storage
  - Browser binding: `BROWSER` - Puppeteer automation
  - AI binding: `AI` - Workers AI for hashtag generation
