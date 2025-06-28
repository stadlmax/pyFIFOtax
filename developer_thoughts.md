# My Thoughts
My name is Max, I wrote most of this code.

## Background
Initially, it was solely meant to handle a few RSU/ESPP-related transactions occuring due to stock events at a broker in the US while employed in Germany but receiving stocks as salary component. Looking at e.g. a few legacy files, it was meant to be maintained through xlsx-files manually created.

At some point, users and I realized that this is too tedious and too rigid. Also, stock splits came into play which weren't previously considered and people wanted to add further exchanges. Due to legacy code, things now were split into two parts. 
- a transactions.xlsx is still kept around to allow the manual approach
- converter scripts convert stock events e.g. at Schwab or IBKR into an updated and more detailed transactions.xlsx 
- tax and AWS reports then are created from reading in the transactions.xlsx

## My Issues
- conversion into transactions.xlsx and later into events being handled by report generation leads to a certain duplication of data-structures
- it is too rigid in a few places
- things like split-handling are not too transparent in many places
- approach through xlsx-files is not too interactive
- generated reports are not in german and don't look too pretty for purposes of presentation to tax offices


## My Plan
- move all current code and most of the current structure into a new folder "legacy" and start from scratch
- goal: have an interactive UI running locally that allows importing transactions (e.g. from Schwab as currently done), verifying imports and automatic modifications (e.g. stock splits), and for generating reports in a nicer form
- unify a few of the data-structures, e.g. the import from schwab might be directly used for handling in the backend without intermediate formats
- keep most of the functionality the same
- ensure sufficient test coverage
- research a good frontend framework to make this work as a first step
