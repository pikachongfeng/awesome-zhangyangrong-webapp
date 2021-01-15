#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__="zhangyangrong"

import logging; logging.basicConfig(level=logging.INFO,)

import asyncio,os,json,time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body='<h1>Awesome</h1>',content_type='text/html')

async def init(loop):
    app=web.Application()
    app.router.add_route('GET','/',index)
    runner=web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner,'127.0.0.1',9000)
    logging.info('server started at http://127.0.0.1:9000...')
    await site.start()

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
