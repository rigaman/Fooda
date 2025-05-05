import pg from 'pg';
const { Pool, Client } = pg;
import { processEvent } from '../service/eventService.mjs';
import { getJson, getSavedOrders, getRewardsConfig, getAccounts } from '../mocks/testJson.mjs';

getRewardsConfig

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
    });

    afterEach(() => {
        jest.clearAllMocks();
    });
    
    it('Test Order insert', async () => {

        const orders = getSavedOrders();

        const json = getJson();

        const rewardsConfig = getRewardsConfig();

        const accounts = getAccounts();

        client.query.mockResolvedValueOnce({ rows: accounts });

        client.query.mockResolvedValueOnce({ rows: rewardsConfig });
       //client.query.mockResolvedValueOnce({ rows: orders }); 
        // Mock the query to return the expected orders
        client.query.mockResolvedValue({ rows: orders });

        const result  = await processEvent(json);

        expect(client.query).toHaveBeenNthCalledWith(1, 'SELECT Id, Name FROM Account WHERE  Name IN ($1)', ['Jessica', 'Will', 'Elizabeth']);
        expect(client.query).toHaveBeenNthCalledWith(2, 'SELECT Id, StartTime, EndTime, RewardPoints FROM RewardsConfig');

        expect(result[0].name).toBe('Jessica');
        expect(result[0].totalRewards).toBe(22);
        expect(result[0].averageRewards).toBe(11);
        expect(result[0].orderCount).toBe(2);

        expect(result[1].name).toBe('Will');
        expect(result[1].totalRewards).toBe(3);
        expect(result[1].averageRewards).toBe(1.5);
        expect(result[1].orderCount).toBe(2);

        expect(result[2].name).toBe('Elizabeth');
        expect(result[2].totalRewards).toBe(0);
        expect(result[2].averageRewards).toBe(0);
        expect(result[2].orderCount).toBe(0);

    });
    it('should fail if parameter passed to function is invalid', async () => {

        const orders = getSavedOrders();

        const json = getJson();

        const rewardsConfig = getRewardsConfig();

        const accounts = getAccounts();

        client.query.mockResolvedValueOnce({ rows: accounts });

        client.query.mockResolvedValueOnce({ rows: rewardsConfig });
        client.query.mockResolvedValueOnce({ rows: orders }); 

        try {

            const result  = await processEvent([]);

        } catch (error) {
            expect(error.message).toBe('Invalid or empty events array');
        }

    });
});