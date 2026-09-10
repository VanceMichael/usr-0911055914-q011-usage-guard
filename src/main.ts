import Koa from 'koa';
import Router from '@koa/router';

const app = new Koa();
const router = new Router();
router.get('/health', ctx => { ctx.body = { status: 'ok' }; });
router.post('/v1/usage', ctx => { ctx.status = 501; ctx.body = { status: '待实现' }; });
app.use(router.routes()).listen(8080, '0.0.0.0');
