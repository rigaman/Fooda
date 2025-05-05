import { openConnecton, closeConnection, client } from '../datalayer/connection.mjs';
import { processAccounts } from '../processor/accountProcessor.mjs';
import { processRewards } from '../processor/rewardsProcessor.mjs';
import { processOrders, getCustomersOrderStats } from '../processor/orderProcessor.mjs';


export async function processEvent(jsonData) {
    if (!jsonData || !jsonData.events || !Array.isArray(jsonData.events) || jsonData.events.length === 0) {
        throw new Error('Invalid or empty events array');
    }

    // Initialize customerReport
    let customerReport = [];
    try {
        // Connect to the database
        await openConnecton();

        // Process accounts
        const accountMap = await processAccounts(jsonData);


        // Process rewards
        const orders = await processRewards(jsonData);

        // Process orders
        const orderResults = await processOrders(orders, accountMap);

        //resoleve jsonData with ids from accounts map 
        jsonData.events.forEach(event => {
            if (accountMap.has(event.name) && event.action === 'new_customer') {
                event.accountId = accountMap.get(event.name).Id;
            }
        });
        // remove new_orders from events 
        // jsonData.events = jsonData.events.filter(event => event.action !== 'new_order');
        // //add orders to jsonData
        // jsonData.events = jsonData.events.concat(orderResults);
        
        customerReport = jsonData.events.filter(event => event.action === 'new_customer');

        customerReport = await getCustomersOrderStats(customerReport);
        
    } catch (error) {
        console.error('Error processing event:', error);
        throw error;
    } finally {
        await closeConnection();
    }
    // Return the processed report
    return customerReport;
}