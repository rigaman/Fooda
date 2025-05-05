import { getRewardsConfig } from '../datalayer/rewards.mjs';

export async function processRewards(jsonData) {

    const defaultConfig = .25; // Default points per dollar if no config matches

    const rewardMin = 3; // Minimum points to be awarded
    const rewardMax = 20; // Maximum points to be awarded

    const ordersArray = jsonData && jsonData.events ? jsonData.events.filter(event => event.action === 'new_order') : [];

    //get rewards configuration from the database
    const rewardsConfig = await getRewardsConfig();

    for (const order of ordersArray) {
        // order reward should be calculated based on schedule 
        // 12pm - 1pm - 1 point per $3 spent
        // 11am - 12pm and 1pm - 2pm - 1 point per $2 spent
        // 10am - 11am and 2pm - 3pm - 1 point per $1 spent
        // Any other time - 0.25 points per $1 spent

        //Any orders that result in rewards points less than 3 or greater than 20 do not receive points. 
        // All rewards points are round up to the next integer.

        order.amount = order.action === 'new_order' ? parseFloat(order.amount || 0) : order.amount; // Ensure amount is a number

        if (order.amount >= rewardMin && order.amount <= rewardMax) {
            //set default reward value
            order.reward = Math.round(order.amount *  defaultConfig); // Default points per dollar
    
            const time = new Date(order.timestamp).getHours() + ':' +
                String(new Date(order.timestamp).getMinutes()).padStart(2, '0'); // Format time as HH:MM

            // Iterate through the rewards configuration to find a matching time slot
            for (const config of rewardsConfig.rows) {

                // Convert config time strings to Date objects for comparison
                const startTime = new Date(`1970-01-01T${config.TimeStart}:00`);
                const endTime = new Date(`1970-01-01T${config.TimeEnd}:00`);

                if (time >= config.TimeStart && time <= config.TimeEnd) {
                    // Calculate reward based on the configuration
                    const amountSpent = order.amount || 0;

                    order.reward_reason = `Order placed between ${config.TimeStart} and ${config.TimeEnd} qualifies for ${config.RewardPoints} points per $${config.DollarAmount} spent`;

                    if (config.RewardPoints) {
                        order.reward = Math.ceil((amountSpent / config.DollarAmount) * config.RewardPoints);
                    }
                    break; // Exit loop once the matching config is found
                }
            }
        } else {
            order.reward = 0; // No reward for amounts outside the defined range
            order.reward_reason = 'Order amount does not qualify for rewards points';
        }
    }

    return ordersArray;

}