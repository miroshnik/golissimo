/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.json`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

interface Env {
	KV: KVNamespace;
	// ... other binding types
}

interface RedditPost {
	id: string;
	title: string;
	videoUrl: string | null;
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		return Response.json(await env.KV.get('processedPosts', 'json'));
	},

	async scheduled (event, env, ctx) {
		console.log(`Scheduled event ${event}`);

		let processedPosts: Record<string, number> = await env.KV.get('processedPosts', 'json') ?? {}; // id => timestamp

		try {
			const response = await fetch('https://www.reddit.com/r/soccer/new.json?limit=25', {
				headers: { 'User-Agent': 'Mozilla/5.0' }
			});

			if (!response.ok) {
				console.error(`Ошибка HTTP: ${response.status}`);

				return;
			}

			const data: Record<any, any> = await response.json();

			const mediaPosts: RedditPost[] = data.data.children
				.filter((post: any) => {
					return post.data.link_flair_text === 'Media' && !processedPosts[post.data.id];
				})
				.map((post: any) => {
					const title = post.data.title;
					const videoUrl = post.data.media?.reddit_video?.fallback_url || post.data.url;
					const id = post.data.id;

					return { id, title, videoUrl };
				});

			for (const post of mediaPosts.reverse()) {
				console.log(`ID: ${post.id}`);
				console.log(`Title: ${post.title}`);

				processedPosts[post.id] = Date.now();
			}

			// Cleanup expired IDs
			const currentTime = Date.now()
			const expirationTime = 24 * 60 * 60 * 1000 // Храним ID сутки

			for (const [id, timestamp] of Object.entries(processedPosts)) {
				if (currentTime - timestamp > expirationTime) {
					console.log('Delete expired post', { id, timestamp })
					delete processedPosts[id]
				}
			}

			await env.KV.put('processedPosts', JSON.stringify(processedPosts));
		} catch (error) {
			console.error(error);
		}
	}
} satisfies ExportedHandler<Env>;
