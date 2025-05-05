import e from "express";

export function getJson() {
    return {
        "events": [
            {
                "action": "new_customer",
                "name": "Jessica",
                "timestamp": "2020-07-01T00:00:00-05:00"
            },
            {
                "action": "new_customer",
                "name": "Will",
                "timestamp": "2020-07-01T01:00:00-05:00"
            },
            {
                "action": "new_customer",
                "name": "Elizabeth",
                "timestamp": "2020-07-01T12:00:00-05:00"
            },
            {
                "action": "new_order",
                "customer": "Jessica",
                "amount": 12.5,
                "timestamp": "2020-07-01T12:15:57-05:00"
            },
            {
                "action": "new_order",
                "customer": "Jessica",
                "amount": 16.5,
                "timestamp": "2020-07-01T10:01:00-05:00"
            },
            {
                "action": "new_order",
                "customer": "Will",
                "amount": 8.9,
                "timestamp": "2020-07-01T12:20:00-05:00"
            },
            {
                "action": "new_order",
                "customer": "Will",
                "amount": 1.5,
                "timestamp": "2020-07-01T12:21:00-05:00"
            }
        ]
    };


}
export function getCustomerMap() {
    return new Map([
        ["Jessica", { Id: 1, name: "Jessica" }],
        ["Will", { Id: 2, name: "Will" }],
        ["Elizabeth", { Id: 3, name: "Elizabeth" }]
    ]);
}
export function getAccounts() {
    return [
        { Id: 1, Name: "Jessica" },
        { Id: 2, Name: "Will" },
        { Id: 3, Name: "Elizabeth" }
    ];
}
export function getOrders() {
    return [
        {   
            id: 1,
            action: "new_order",
            customer: "Jessica",
            amount: 12.5,
            timestamp: "2020-07-01T12:15:57-05:00",
            reward: 3
        },
        {   
            id: 2,
            action: "new_order",
            customer: "Jessica",
            amount: 16.5,
            timestamp: "2020-07-01T10:01:00-05:00",
            reward: 17
        },
        {
            id: 3,
            action: "new_order",
            customer: "Will",
            amount: 8.9,
            timestamp: "2020-07-01T12:20:00-05:00",
            reward: 3
        },
        {   id: 4,
            action: "new_order",
            customer: "Will",
            amount: 1.5,
            timestamp: "2020-07-01T12:21:00-05:00",
            reward: 0
        }
    ];
}

export function getSavedOrders() {
    "Id", "Account", "Amount", "Timestamp", "RewardAmount"
    return [
        { Id: 1, Account: 1, Amount: 12.5, Timestamp: "2020-07-01T12:15:57-05:00", RewardAmount: 5 },
        { Id: 2, Account: 1, Amount: 16.5, Timestamp: "2020-07-01T10:01:00-05:00", RewardAmount: 17 },
        { Id: 3, Account: 2, Amount: 8.9, Timestamp: "2020-07-01T12:20:00-05:00", RewardAmount: 3 },
        { Id: 4, Account: 2, Amount: 1.5, Timestamp: "020-07-01T12:21:00-05:00", RewardAmount: 0 }

    ];
}
        // order reward should be calculated based on schedule 
        // 12pm - 1pm - 1 point per $3 spent
        // 11am - 12pm and 1pm - 2pm - 1 point per $2 spent
        // 10am - 11am and 2pm - 3pm - 1 point per $1 spent
        // Any other time - 0.25 points per $1 spent
export function getRewardsConfig() {
    return [
        { id: 1, TimeStart: '12:00', TimeEnd: '13:00', RewardPoints: 1, DollarAmount: 3 },
        { id: 2, TimeStart: '11:00', TimeEnd: '12:00', RewardPoints: 1, DollarAmount: 2 },
        { id: 3, TimeStart: '13:00', TimeEnd: '14:00', RewardPoints: 1, DollarAmount: 2 },
        { id: 4, TimeStart: '10:00', TimeEnd: '11:00', RewardPoints: 1, DollarAmount: 1 },
        { id: 5, TimeStart: '14:00', TimeEnd: '15:00', RewardPoints: 1, DollarAmount: 1 }
    ];
}