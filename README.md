# Redis Replica  

[![Java](https://img.shields.io/badge/Language-Java-007396?logo=java&logoColor=white)](https://www.java.com/)  
[![Redis](https://img.shields.io/badge/Database-Redis-D82C20?logo=redis&logoColor=white)](https://redis.io/)  

## Project Overview  

This project is a custom implementation of a Redis-like in-memory key-value store written in **Java**. It provides basic Redis server and client functionality, allowing users to interact with the server through a CLI interface to execute commands.  

## Features  
- **In-Memory Key-Value Storage**: Supports core Redis-like operations for managing key-value pairs.  
- **Server-Client Architecture**: Simulates a Redis server and client interaction.  
- **Command Execution**: The client can send supported commands to the server, which processes and responds accordingly.  

---

## How to Run  

### Prerequisites  
- Java Development Kit (JDK) 11 or higher installed.  
- Unix-based environment (Linux/Mac) or WSL for running the `.sh` scripts.  

### Steps  

1. **Clone the Repository**  
   ```bash
   git clone https://github.com/Indrahas/redis-replica.git 
   cd redis-replica
   ```
   
2. **Start the Redis Server** - 
Run the redis_server.sh script to launch the server: 
 ```bash
 ./redis_server.sh 
 ```

3. **Start the Redis Client** - 
In a new terminal, run the redis_cli.sh script to interact with the server:
 ```bash
 ./redis_cli.sh 
 ```

### Example
![Alt text](https://i.ibb.co/vcPxDtM/redis-replica.png "a title") 


  
  


