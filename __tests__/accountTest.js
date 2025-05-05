import pg from 'pg';
const { Pool, Client } = pg;
import { processAccounts } from '../processor/accountProcessor.mjs';
import { getJson } from '../mocks/testJson.mjs';
import { openConnecton, closeConnection } from '../datalayer/connection.mjs';

jest.mock('pg', () => {
    const mClient = {
      connect: jest.fn(),
      query: jest.fn(),
      end: jest.fn(),
    };
    return { Client: jest.fn(() => mClient) };
  });

describe('Account processor Tests', () => {
    let client;
    beforeEach(async () => {
        client = new Client();
        await openConnecton();
    });

    afterEach(async () => {
        jest.clearAllMocks();
        await closeConnection();
    });

    it('Happy path it will get and insert accounts', async () => {
        const json = getJson();
        client.query.mockResolvedValueOnce({ rows: [{ id: 1, Name: 'Jessica'}] });
        client.query.mockResolvedValueOnce({ rows: [{ id: 2, Name: 'Will'}, { id: 3, Name: 'Elizabeth'}] });
        const data = await processAccounts(json);

        expect(client.query).toHaveBeenNthCalledWith(1, "SELECT Id, Name FROM Account WHERE  Name IN ($1)", ["Jessica", "Will", "Elizabeth"]);
        expect(client.query).toHaveBeenNthCalledWith(2, "INSERT INTO Account (Name) VALUES ('Will'),('Elizabeth') RETURNING Id, Name");    

        expect(data).toBeInstanceOf(Map);
        expect(data.size).toBe(3);
        expect(data.has('Jessica')).toBe(true);
        expect(data.has('Will')).toBe(true);
        expect(data.has('Elizabeth')).toBe(true);
        expect(data.get('Jessica').id).toBe(1);
        expect(data.get('Will').id).toBe(2);
        expect(data.get('Elizabeth').id).toBe(3);

    });

    it('should fail', async () => {
        const json = {events: []};
        try {
            const data = await processAccounts(json);
        }
        catch (error) {
            expect(error.message).toBe('Name is required');
        }

    });

    it('should return empty map if there are no customers in the event json', async () => {

        const json = getJson();

        //filter out customers from json
        json.events = json.events.filter(event => event.action !== 'new_customer');

        const data = await processAccounts(json);
        expect(data).toBeInstanceOf(Map);
        expect(data.size).toBe(0);
    
    });
});