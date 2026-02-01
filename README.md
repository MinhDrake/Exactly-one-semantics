# Exactly-Once Semantics
Kafka's Exactly-Once Semantics (Transaction) using Java.

## Structure
- `InputProducer`: Generates numbers 1-10.
- `EOSStreamProcessor`: Consumes numbers, multiplies by 2, and produces to output. **Simulates random crashes** to prove resilience.
- `OutputVerifier`: Consumes output and checks for duplicates.

## How to Run

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Build Project
```bash
mvn clean package
```

### 3. Run Components (In separate terminals)

**Terminal 1: The Verifier (Watcher)**
```bash
mvn exec:java -Dexec.mainClass="com.demo.eos.OutputVerifier"
```

**Terminal 2: The Processor (Worker)**
*This will crash randomly. Auto-restart script is not included, please re-run manually when it crashes.*
```bash
mvn exec:java -Dexec.mainClass="com.demo.eos.EOSStreamProcessor"
```

**Terminal 3: The Input (Trigger)**
```bash
mvn exec:java -Dexec.mainClass="com.demo.eos.InputProducer"
```

## Expected Result
- Even if `EOSStreamProcessor` crashes and you restart it multiple times.
- `OutputVerifier` will **NEVER** show "❌ DUPLICATE DETECTED".
- It will only show "✅ Received Valid" for exactly one result per input.


Huge thanks to:
- You, for reading this README 😄
- Seglo for the amazing idea.