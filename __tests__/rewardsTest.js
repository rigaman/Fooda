import pg from 'pg';
const { Pool, Client } = pg;
import { processRewards } from '../processor/rewardsProcessor.mjs';
import { getJson, getSavedOrders, getRewardsConfig, getAccounts } from '../mocks/testJson.mjs';
import { openConnecton, closeConnection } from '../datalayer/connection.mjs';

jest.mock('pg', () => {
    const mClient = {
      connect: jest.fn(),
      query: jest.fn(),
      end: jest.fn(),
    };
    return { Client: jest.fn(() => mClient) };
  });


describe('Rewards processor Tests', () => {
    let client;
    beforeEach(() => {
        client = new Client();
        openConnecton();
    });

    afterEach(() => {
        jest.clearAllMocks();
        closeConnection();
    });

    it('Test rewards calculation', async () => {

        // order reward should be calculated based on schedule 
        // 12pm - 1pm - 1 point per $3 spent
        // 11am - 12pm and 1pm - 2pm - 1 point per $2 spent
        // 10am - 11am and 2pm - 3pm - 1 point per $1 spent
        // Any other time - 0.25 points per $1 spent

        const json = getJson();
        const rewardsConfig = getRewardsConfig();

        // Mock the rewards configuration query to return the expected data
        client.query.mockResolvedValueOnce({ rows: rewardsConfig });


        const data = await processRewards(json);

        expect(client.query).toHaveBeenCalledWith('SELECT Id, StartTime, EndTime, RewardPoints FROM RewardsConfig');
        expect(data).toBeInstanceOf(Array);
        expect(data.length).toBe(4);

        expect(data[0].reward).toBe(5); 
        expect(data[0].reward_reason).toBe('Order placed between 12:00 and 13:00 qualifies for 1 points per $3 spent');
        expect(data[1].reward).toBe(17);
        expect(data[1].reward_reason).toBe('Order placed between 10:00 and 11:00 qualifies for 1 points per $1 spent');
        expect(data[2].reward).toBe(3);
        expect(data[2].reward_reason).toBe('Order placed between 12:00 and 13:00 qualifies for 1 points per $3 spent');
        expect(data[3].reward).toBe(0);
        expect(data[3].reward_reason).toBe('Order amount does not qualify for rewards points');

    });

    it('Test rewards calculation should use default config', async () => {

        const json = getJson();

        // Mock the rewards configuration query to return the expected data
        client.query.mockResolvedValueOnce({ rows: [] });


        const data = await processRewards(json);

        expect(client.query).toHaveBeenCalledWith('SELECT Id, StartTime, EndTime, RewardPoints FROM RewardsConfig');
        expect(data).toBeInstanceOf(Array);
        expect(data.length).toBe(4);

        expect(data[0].reward).toBe(3); 
        expect(data[1].reward).toBe(4);
        expect(data[2].reward).toBe(2);
        expect(data[3].reward).toBe(0);

    });
    it('Test rewards should be 0 if amount is greater tnan 20', async () => {

        const json = getJson();
        const rewardsConfig = getRewardsConfig();
        // Mock the rewards configuration query to return the expected data
        client.query.mockResolvedValueOnce({ rows: rewardsConfig });

        // set all order amount to 21 to test max value
        json.events.forEach(event => {
            if (event.action === 'new_order') {
                event.amount = 21; // Set all order amounts to 21
            }
        });  

        const data = await processRewards(json);

        expect(client.query).toHaveBeenCalledWith('SELECT Id, StartTime, EndTime, RewardPoints FROM RewardsConfig');
        expect(data).toBeInstanceOf(Array);
        expect(data.length).toBe(4);

        expect(data[0].reward).toBe(0); 
        expect(data[1].reward).toBe(0);
        expect(data[2].reward).toBe(0);
        expect(data[3].reward).toBe(0);

    });
});