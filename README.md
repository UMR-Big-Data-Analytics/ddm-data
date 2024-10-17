# ddm-akka
Akka example and homework code for the "Big Data Systems" lecture.

## Requirements
- Java Version >= 11
- Maven Compiler Version >= 3.8.1

## Getting started
1. Clone repo
  ```
  git clone https://github.com/UMR-Big-Data-Analytics/ddm-akka.git
  ```
        
2. Decompress test data
  ```
  cd ddm-akka/data
  unzip TPCH.zip
  ```

3. Build project with maven
  ```
  cd ..
  mvn package
  ```

3. Read the program documentation
  ```
  java -jar target/ddm-akka-1.0.jar
  ```

4. First run
  ```
  java -jar target/ddm-akka-1.0.jar master
  ```

5. Distributed run (locally on one machine)
  ```
  // Run a master
  java -Xms2048m -Xmx2048m -jar target/ddm-akka-1.0.jar master -w 0
  // Run a worker (repeat for multiple workers)
  java -Xms2048m -Xmx2048m -jar target/ddm-akka-1.0.jar worker -w 1 
  ```

`-Xms` and `-Xmx` are options for the Java Virtual Machine [to configure initial and maximum heap size](https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xms). To ensure that your program runs on the Pi cluster, make it no greater than two gigabytes (`-Xmx=2048m` or `-Xmx=2g`).

6. Distributed run (on multiple machines)
  ```
  // Run a master
  java -Xms2048m -Xmx2048m -jar target/ddm-akka-1.0.jar master -w 0 -h <your-ip-address>
  // Run a worker (repeat for multiple workers)
  java -Xms2048m -Xmx2048m -jar target/ddm-akka-1.0.jar worker -w 1 -mh <master-host-ip> -h <your-ip-address>
  ```

**Note that you need to substitute `<your-ip-address>` and `<master-host-ip>` with your and the master's IP address, respectively.** You can use websites like [whatismyipaddress.com](https://whatismyipaddress.com/) or command-line utilities like `hostname -I` and `ifconfig` to get these IP addresses.

`-Xms` and `-Xmx` are options for the Java Virtual Machine [to configure initial and maximum heap size](https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xms). To ensure that your program runs on the Pi cluster, make it no greater than two gigabytes (`-Xmx=2048m` or `-Xmx=2g`).

## Hints

1. Run `java -jar target/ddm-akka-1.0.jar` without arguments to have a help text printed to your console. It will describe all parameters in detail.
2. Use `java -Xms2048m -Xmx2048m` to restrict your program run to two gigabyte of memory. This ensures that it runs on a Pi cluster.
3. Use `LargeMessageProxy` to send large messages.
4. Use `MemoryUstils` to measure byte sizes and memory usage.
