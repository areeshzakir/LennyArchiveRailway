import { PrismaClient } from '@prisma/client'
import Parser from 'rss-parser'
import cron from 'node-cron'

const prisma = new PrismaClient()
const parser = new Parser({
    customFields: {
        item: [
            ['itunes:duration', 'itunes_duration'],
            ['itunes:author', 'itunes_author'],
            ['dc:creator', 'creator'],
            ['itunes:image', 'itunes_image'],
        ]
    }
})

const FEEDS = [
    "https://api.substack.com/feed/podcast/10845/s/215368/private/56c9d04c-5a32-4a8d-b4c8-f1279e93fd45.rss",
    "https://api.substack.com/feed/podcast/10845/private/56c9d04c-5a32-4a8d-b4c8-f1279e93fd45.rss",
    "https://api.substack.com/feed/podcast/10845/s/198869/private/56c9d04c-5a32-4a8d-b4c8-f1279e93fd45.rss"
]

async function syncFeeds() {
    console.log(`[${new Date().toISOString()}] Starting podcast sync...`)
    let totalUpserted = 0;

    for (const feedUrl of FEEDS) {
        try {
            console.log(`Fetching feed: ${feedUrl.split('?')[0]}...`)
            const feed = await parser.parseURL(feedUrl)

            for (const item of feed.items) {
                if (!item.guid) {
                    console.warn(`Item missing guid: ${item.title}`);
                    continue;
                }

                // Clean up text slightly if necessary
                const description = item.contentSnippet || item.content || (item as any).description || ''
                const pubDate = item.pubDate ? new Date(item.pubDate) : new Date()

                // Extract Enclosure Data
                let audioUrl = null, audioLength = null, audioType = null;
                if (item.enclosure) {
                    audioUrl = item.enclosure.url;
                    audioLength = item.enclosure.length;
                    audioType = item.enclosure.type;
                }

                // Upsert into Postgres
                await prisma.episode.upsert({
                    where: { guid: item.guid },
                    update: {
                        title: item.title,
                        description: description,
                        pub_date: pubDate,
                        audio_url: audioUrl,
                    },
                    create: {
                        guid: item.guid,
                        feed_url: feedUrl,
                        title: item.title || 'Untitled Episode',
                        description: description,
                        pub_date: pubDate,
                        link: item.link,
                        audio_url: audioUrl,
                        audio_length: audioLength,
                        audio_type: audioType,
                        itunes_duration: item['itunes_duration'],
                        itunes_author: item['itunes_author'],
                        creator: item['creator'],
                        itunes_image: item['itunes_image'] ? item['itunes_image']['$']?.href : null,
                    }
                })
                totalUpserted++;
            }
            console.log(`Successfully synced feed.`)

        } catch (error) {
            console.error(`Error syncing feed ${feedUrl}:`, error)
        }
    }

    console.log(`[${new Date().toISOString()}] Sync complete. Upserted ${totalUpserted} items across all feeds.`)
}

// Ensure database connection starts up
async function main() {
    console.log("Starting Podcast Sync Worker")

    // Run once immediately on startup
    await syncFeeds()

    // Then schedule to run every hour (0 * * * *)
    cron.schedule('0 * * * *', async () => {
        await syncFeeds()
    })
}

main()
    .catch((e) => {
        console.error("Worker failed starting up:", e)
        process.exit(1)
    })
