import { openConnecton, closeConnection, client } from '../datalayer/connection.mjs';
import { processAccounts } from '../processor/accountProcessor.mjs';
import { processRewards } from '../processor/rewardsProcessor.mjs';
import { processOrders } from '../processor/orderProcessor.mjs';


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
        jsonData.events = jsonData.events.filter(event => event.action !== 'new_order');
        //add orders to jsonData
        jsonData.events = jsonData.events.concat(orderResults);


        //Generate a report containing each customer with total rewards and average rewards per order.
        //Report output should order users by total rewards most to least.
        const customerOrders = {};
        orderResults.forEach(order => {
            const customerId = order.Account;
            if (!customerOrders[customerId]) {
                customerOrders[customerId] = { totalRewards: 0, orderCount: 0 };
            }
            customerOrders[customerId].totalRewards += order.RewardAmount;
            customerOrders[customerId].orderCount += 1;
        });
        // Calculate average rewards per order
        for (const customerId in customerOrders) {
            if (customerOrders[customerId].orderCount > 0) {
                customerOrders[customerId].averageRewards = 
                    (customerOrders[customerId].totalRewards / customerOrders[customerId].orderCount);
            }
        }

        customerReport = jsonData.events.filter(event => event.action === 'new_customer');

        for (const customer of customerReport) {
            const customerId = customer.accountId;
            if (customerOrders[customerId]) {
                customer.totalRewards = customerOrders[customerId].totalRewards;
                customer.averageRewards = customerOrders[customerId].averageRewards;
                customer.orderCount = customerOrders[customerId].orderCount;
            } else {
                customer.totalRewards = 0;
                customer.averageRewards = 0;
                customer.orderCount = 0;
            }
        }

        // Sort customers by total rewards
        customerReport.sort((a, b) => {
            return b.totalRewards - a.totalRewards || a.name.localeCompare(b.name);
        });
        
    } catch (error) {
        console.error('Error processing event:', error);
        throw error;
    } finally {
        await closeConnection();
    }
    // Return the processed jsonData
    return customerReport;
}