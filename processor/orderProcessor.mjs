import { insertOrders, getOrdersByCustomerIds } from '../datalayer/order.mjs';
// orders array sample
// const ordersArray = [
//     {
//         action: "new_order",
//         customer: "Jessica",
//         amount: 8.9,
//         timestamp: "2020-07-01T12:20:00-05:00",              
//         reward: 3 
//     }]
// accounts map {customerName : accountObject}
// account object sample {Id: 1, name: "Jessica"}

export async function processOrders(ordersArray, accountMap) {

    if (!ordersArray || !ordersArray.length) {
        throw new Error('Orders array is required');
    }
    if (!accountMap || !accountMap instanceof Map || accountMap.keys().length === 0) {
        throw new Error('Account map is required');
    }
    //create array of orders to insert
    const ordersToInsert = [];

    for (const order of ordersArray) {
        if (order.action === 'new_order' && order.customer) {
            const account = accountMap.get(order.customer);
            if (account) {
                //create order object to insert
                const orderToInsert = {
                    account: account.Id, // Assuming account is an ID
                    amount: order.amount,
                    timestamp: order.timestamp,
                    reward: order.reward || 0 // Default to 0 if not provided
                };
                ordersToInsert.push(orderToInsert);
            } else {
                console.warn(`Account for customer ${order.customer} not found`);
            }
        }
    }
    // Insert orders into the database
    const insertedOrders = await insertOrders(ordersToInsert);
    return insertedOrders.rows; // Return the inserted orders
    
}

export async function getCustomersOrderStats(customers) {

    //create array of orders to insert
    const ordersToInsert = [];

    const orders = await getOrdersByCustomerIds(customers.map( customer => customer.accountId));
    
    //Generate a report containing each customer with total rewards and average rewards per order.
    //Report output should order users by total rewards most to least.
    const customerOrders = {};
    orders.rows.forEach(order => {
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

    for (const customer of customers) {
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
    customers.sort((a, b) => {
        return b.totalRewards - a.totalRewards || a.name.localeCompare(b.name);
    });


    return customers;
    
}