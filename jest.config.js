export default {
    testEnvironment: 'jest-environment-node',
    "transform": {
        ".*.js": "<rootDir>/node_modules/babel-jest"
    },
    transformIgnorePatterns: ['node_modules'],
    testMatch: [
        "<rootDir>/tests/*.test.js",
    ]
};