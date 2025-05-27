# Ethereum Blockchain Analysis

This project was developed as part of the MSc in Big Data Science with Machine Learning Systems at Queen Mary University of London. It focuses on analysing Ethereum blockchain data using distributed processing frameworks — Hadoop MapReduce (via `mrjob`) and Apache Spark — to study network behaviour, miner activity, and contract interactions.

## Project Structure

- `partA/`  
  Time-based analysis of Ethereum transactions (monthly volume and average value)

- `partB/`  
  Identification of the top 10 most popular smart contracts by total Ether received

- `partC/`  
  Ranking of top miners by total block size mined

- `partD/`  
  - `comparative_spark.py`: Spark vs Hadoop comparison on smart contract analysis  
  - `ethereum_fork.py`: Gas price analysis during the July 2016 DAO fork  
  - `gas1.py`, `gas2.py`, `gas3.py`: Analysis of gas usage and contract complexity over time

- `report.pdf`  
  Original academic submission including methodology, job results, and charts

## Technologies Used

- Python
- MRJob (Python-based MapReduce)
- Apache Spark (PySpark)
- Excel (for plotting and post-processing)

## How to Run

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run MapReduce scripts:
```bash
python partA/partA1.py input_file.csv > output.txt
```

3. Run Spark jobs:
```bash
spark-submit partD/comparative_spark.py
```

> **Note**: These scripts were executed on Queen Mary University's distributed computing cluster. Input files were accessed from `/data/ethereum/`. Paths should be updated if running locally.

## Results Summary

- Spark executed the smart contract aggregation task in ~214s, compared to ~30 minutes with Hadoop
- Monthly transaction volume and average values were analysed and plotted using MapReduce
- Ethereum’s most used smart contract received 84 billion wei
- The top miner processed nearly 24 GB of block data
- Ethereum network activity spiked following the July 2016 DAO fork

---

## Dataset Schema (Reference)

**Blocks**
- `number`: The block number  
- `hash`: Hash of the block  
- `parent_hash`: Hash of the parent of the block  
- `nonce`: Nonce that satisfies the difficulty target  
- `miner`: The address of the beneficiary to whom the mining rewards were given  
- `size`: The size of this block in bytes  
- `difficulty`: Integer of the difficulty for this block  
- `gas_limit`: The maximum gas allowed in this block  
- `gas_used`: The total used gas by all transactions in this block  
- `timestamp`: The timestamp for when the block was collated  

**Transactions**
- `hash`: Hash of the transaction  
- `block_number`: Block number where this transaction was in  
- `from_address`: Address of the sender  
- `to_address`: Address of the receiver  
- `value`: Value transferred in Wei (the smallest denomination of Ether)  
- `gas`: Gas provided by the sender  
- `gas_price`: Gas price provided by the sender  
- `timestamp`: Timestamp the associated block was registered at  

**Contracts**
- `address`: Address of the contract  
- `bytecode`: Code for the Ethereum contract  
- `function_sighashes`: Function signature hashes of a contract  
- `is_erc20`: Whether this contract is an ERC20 token  
- `is_erc721`: Whether this contract is an ERC721 token  
- `block_number`: Block number where this contract was created
