import pg from 'pg';
const { Pool, Client } = pg;
import { processOrders } from '../processor/orderProcessor.mjs';
import { getCustomerMap, getSavedOrders, getOrders  } from '../mocks/testJson.mjs';
import { openConnecton, closeConnection } from '../datalayer/connection.mjs';

jest.mock('pg', () => {
    const mClient = {
      connect: jest.fn(),
      query: jest.fn(),
      end: jest.fn(),
    };
    return { Client: jest.fn(() => mClient) };
  });


describe('Orders processor Tests', () => {
    let client;
    beforeEach(() => {
        client = new Client();
        openConnecton();
    });

    afterEach(() => {
        jest.clearAllMocks();
        closeConnection();
    });
    it('Happy path', async () => {

        const orders = getOrders();
        const savedOrders = getSavedOrders();

        client.query.mockResolvedValueOnce({ rows: savedOrders});

        const customerMap = getCustomerMap();

        const data = await processOrders(orders, customerMap );
        expect(client.query).toHaveBeenCalled();
        expect(data).toBeInstanceOf(Array);
        expect(data.length).toBe(4);
    });
    it('it should fail becaus eorders is empty', async () => {

        const orders = [];
        const savedOrders = getSavedOrders();

        client.query.mockResolvedValueOnce({ rows: savedOrders});

        const customerMap = getCustomerMap();
        try {
            const data = await processOrders(orders, customerMap );
        } catch (error) {
            expect(error.message).toBe('Orders array is required');
        }
    });
    it('it should fail becaus eorders is empty', async () => {

        const orders = getOrders();
        const savedOrders = getSavedOrders();

        client.query.mockResolvedValueOnce({ rows: savedOrders});

        const customerMap = new Map(); // empty customer map
        try {
            const data = await processOrders(orders, customerMap );
        } catch (error) {
            expect(error.message).toBe('Account map is required');
        }
    });
});