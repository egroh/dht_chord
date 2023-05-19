# DHT-2 Initial Project Report

1. Team information:
    - Team name: Ants or something
    - Team members: Valentin Metz, Eddie Groh
    - Module: Distributed Hash Table

2. Language and OS:
    - Rust
        - Fast execution, no garbage collector
        - Strong safety guarantees to prevent common security issues like buffer overflows
        - Available libraries for fast and safe network interaction
    - Arch based Linux (Manjaro)
        - Both of us use this as our main OS
        - Up-to-date packages
        - Well documented low level hardware features
        - Directly compatible with all linux based servers
        - Rust toolchain works out of the box and is well-supported for this OS
        - As Cargo statically links crates, and we will compile for a generic linux target, we will ideally be
          relatively OS independent
        - OS for final deployment might be different, if we find any significant reasons for that
    - Maybe Docker
        - Portable deployment environment
        - Ensures correct dependencies are installed

3. Build system:
    - Cargo

4. Quality control:
    - Rust is a language designed for performance and safety
    - The borrow checker will ensure memory safety and prevent race conditions
    - We will provide unit tests through cargo-test
    - We will provide benchmarks through cargo-bench
    - We will provide our documentation through cargo-rustdoc

5. Libraries:
    - Tokio will provide the backbone for our asynchronous network infrastructure
    - Crypto libraries will be chosen after further evaluation

6. License:
    - GNU Affero General Public License (AGPL) v3.0
        - We want all users of the network to have full access to the source code used to run the network
        - This license allows for improvement of the network as any peer can modify the code and publish the modified
          code
        - We explicitly prohibit distribution of our code in closed source projects
        - https://choosealicense.com/licenses/agpl-3.0/

7. Programming experience:
    - Valentin Metz:
        - Programming languages:
            - Rust
            - Python
            - C++
            - C
            - Java
    - Eddie Groh:
        - Programming languages:
            - Rust beginner
            - C++
            - C
            - Java

8. Planned workload distribution:
    - We have worked together with great success on other projects in the past
    - Our workload will be distributed in fair, dynamic and on demand fashion
    - Project and protocol design are worked on together
    - Implementation of individual software parts will be assigned to one of us as they come up
