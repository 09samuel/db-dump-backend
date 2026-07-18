const request = require('supertest');
const app = require('../app');

describe('GET /health', () => {
  it('should return 200 UP with a timestamp', async () => {
    const res = await request(app)
      .get('/health')
      .expect('Content-Type', /json/)
      .expect(200);

    expect(res.body).toHaveProperty('status', 'UP');
    expect(res.body).toHaveProperty('timestamp');
  });
});
