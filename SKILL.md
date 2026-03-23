---
name: my-attribution
description: "Use ONLY when the user explicitly wants to create, query advertisers, conversions, health, pixel via the my-attribution API. Do NOT use for general questions, web searches, or anything unrelated to my-attribution data operations."
emoji: 🐑
requires:
  bins: [curl, python3]
  env:
  - MY_ATTRIBUTION_API_KEY
primaryEnv: MY_ATTRIBUTION_API_KEY
---
# my-attribution
Attribution tracking for agent advertising

## When to Use
Use ONLY when the user explicitly wants to create, query advertisers, conversions, health, pixel via the my-attribution API.
Triggers: user mentions my-attribution, asks to create, query advertisers, conversions, health, pixel, or references specific my-attribution data.

## When NOT to Use
- general knowledge questions about advertisers, conversions, health, pixel (use web search instead)
- displaying static info the user already has — this skill calls a live API
- anything unrelated to my-attribution — e.g. weather, math, file editing, image generation

## Available Endpoints
- **GET /** — `root`
- **GET /v1/health** — `health`
- **POST /v1/sessions** — `create_session`
- **GET /v1/sessions** — `list_sessions`
- **POST /v1/advertisers** — `create_advertiser`
- **GET /v1/advertisers** — `list_advertisers`
- **POST /v1/conversions** — `create_conversion`
- **GET /v1/reports/{advertiser_id}** — `advertiser_report`
- **GET /v1/reports/{advertiser_id}/conversions** — `advertiser_conversions`
- **GET /v1/stats** — `global_stats`

## Instructions
1. Read API base URL from `$MY_ATTRIBUTION_URL`
2. Read API key from `$MY_ATTRIBUTION_API_KEY`
3. To root: `GET {base_url}/` with `Authorization: Bearer ${key}`
4. To health: `GET {base_url}/v1/health` with `Authorization: Bearer ${key}`
5. To create session: `POST {base_url}/v1/sessions` with `Authorization: Bearer ${key}`
6. To list sessions: `GET {base_url}/v1/sessions` with `Authorization: Bearer ${key}`
7. To create advertiser: `POST {base_url}/v1/advertisers` with `Authorization: Bearer ${key}`
8. To list advertisers: `GET {base_url}/v1/advertisers` with `Authorization: Bearer ${key}`
9. Parse JSON response and present to user
10. On error (401/404/429/500), report status and message

## Error Handling
- **401**: API key missing/invalid — check `MY_ATTRIBUTION_API_KEY`
- **404**: Resource not found — confirm ID/path
- **429**: Rate limited — wait and retry
- **500**: Server error — try again later

### Conversation Example
**User:** "Show me the root"
**Agent:** runs `bash scripts/my-attribution.sh root` → formats GET / response for user.

## Examples
```bash
bash scripts/my-attribution.sh root
bash scripts/my-attribution.sh health '{"name": "example"}'
bash scripts/my-attribution.sh --help
```

## Notes
- All responses are JSON | Auth: `Authorization: Bearer` header | Built with Sheept 🐑
