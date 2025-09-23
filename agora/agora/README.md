1. `metaserver`: uses `tarpc` to implement service discovery. 
    - Each publisher-subscriber is registered at a filesystem-like path. 
    - The metaserver allocates address & ports for each publisher. 
    - Registration logic:
        - Allocate socket for service
        - Initialize a heartbeat client. Deregister service if heartbeat fails. 
2. `rawstream`: uses `tokio-tungstenite` to establish single-publisher-multiple-subscriber data flow. 
    - No interaction with metaserver at all. Purely point-to-point byte streaming logic. 
3. `core`: integrates `metaserver` and `rawstream` to implement publishers and subscribers
    - `publisher` initialization: `stream<T>, path`
        a. Initialize a `metaserver` client and register to obtain socket. 
        b. Initialize a `rawstream` server at obtained socket
        c. Initializes a heartbeat server (at obtained socket??)
        d. Serializes `stream<T>` to `stream<bytes>` and transmits over `rawstream`
    - `subscriber` initialization: `subscribe<T>(path) -> stream<T>`
        a. Initialize `metaserver` client to obtain the service socket. 
        b. Initialize `rawstream` client at obtained socket
        c. Serializes `stream<bytes>` to `stream<T>`


Todos:
1. Heartbeat. 
2. rawstream
3. core