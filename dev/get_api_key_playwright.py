from playwright.async_api import async_playwright
import asyncio

async def get_x_api_key():
    async with async_playwright() as p:
        # Запускаем браузер
        browser = await p.chromium.launch(headless=False)  # Для отладки лучше headless=False
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 720},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        # ✅ Создаём новую страницу
        page = await context.new_page()

        api_key = None

        # Перехватываем запросы на уровне контекста
        async def on_request(route, request):
            nonlocal api_key
            url = request.url
            if "api.av.by" in url and "/offer-types/moto/counters/bike" in url:
                headers = await request.all_headers()
                x_api_key = headers.get('x-api-key') or headers.get('X-API-Key')
                if x_api_key:
                    print(f"✅ Захвачен x-api-key: {x_api_key}")
                    print(f"📌 URL: {url}")
                    api_key = x_api_key
                await route.continue_()
                return
            await route.continue_()

        # Устанавливаем маршрут ДО перехода
        await context.route("**/*", on_request)

        print("🌐 Переходим на https://moto.av.by...")
        try:
            await page.goto("https://moto.av.by", timeout=60000)
        except Exception as e:
            print(f"❌ Ошибка при переходе: {e}")

        # Ждём выполнения API-запросов
        print("⏳ Ожидаем API-запросы...")
        await page.wait_for_timeout(10000)

        await browser.close()
        return api_key

# Запуск асинхронной функции
if __name__ == "__main__":
    api_key = asyncio.run(get_x_api_key())
    print(f"\n🔑 Итоговый x-api-key: {api_key}")