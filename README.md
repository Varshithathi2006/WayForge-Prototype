# 🚌 Bangalore Transit Pipeline 🚇

A real-time transit data pipeline for Bangalore that integrates BMTC (Bus) and BMRCL (Metro) data using Apache Kafka and Pathway Engine for intelligent route optimization.

## 🎯 Overview

This project demonstrates a complete real-time transit data processing pipeline that:

- **Ingests** static and live transit data from BMTC and BMRCL
- **Processes** data through Apache Kafka topics
- **Optimizes** routes using Pathway Engine for fastest, cheapest, eco-friendly, and balanced routes
- **Provides** real-time route recommendations with comprehensive metrics

## 🏗️ Architecture

```
📊 Data Sources → 🔄 Kafka Topics → 🧮 Pathway Engine → 📈 Route Optimization
     ↓                    ↓                ↓                    ↓
  JSON Files         Message Queue    Real-time Processing   Smart Routes
```

### Components

- **Producers**: Read JSON data files and publish to Kafka topics
- **Consumers**: Process Kafka messages using Pathway Engine
- **Route Optimizer**: Compute optimal routes based on multiple criteria
- **Demo Interface**: Interactive CLI for demonstration
- **Monitoring**: Comprehensive logging and error handling

## 📋 Prerequisites

### System Requirements

- **macOS** (tested on macOS 10.15+)
- **Python 3.8+**
- **Apache Kafka** (local installation)
- **Java 8+** (for Kafka)

### Kafka Setup

1. **Install Kafka** (using Homebrew):
   ```bash
   brew install kafka
   ```

2. **Start Zookeeper**:
   ```bash
   brew services start zookeeper
   ```

3. **Start Kafka**:
   ```bash
   brew services start kafka
   ```

4. **Create Topics**:
   ```bash
   # BMTC Topics
   kafka-topics --create --topic bmtc.gtfs_static --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   kafka-topics --create --topic bmtc.fares --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   kafka-topics --create --topic bmtc.vehicle_positions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   
   # BMRCL Topics
   kafka-topics --create --topic bmrcl.gtfs_static --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   kafka-topics --create --topic bmrcl.fares --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   kafka-topics --create --topic bmrcl.train_positions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Navigate to your project directory
cd /path/to/your/project/bangalore_transit_pipeline

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Verify Installation

```bash
# Check if all dependencies are installed
python -c "import kafka, pandas, numpy; print('✅ All dependencies installed')"
```

### 3. Run the Demo

```bash
# Start the interactive demo interface
python demo_interface.py
```

### 4. Alternative: Manual Pipeline

If you prefer to run components separately:

```bash
# Terminal 1: Start Producer
python producers/kafka_producer.py

# Terminal 2: Start Consumer
python consumers/pathway_consumer.py
```

## 📁 Project Structure

```
bangalore_transit_pipeline/
├── 📁 config/
│   └── kafka_config.py          # Kafka and system configuration
├── 📁 producers/
│   └── kafka_producer.py        # Data producers for Kafka topics
├── 📁 consumers/
│   ├── pathway_consumer.py      # Pathway Engine consumer
│   └── route_optimizer.py      # Route optimization logic
├── 📁 data/
│   ├── 📁 static/               # Static transit data
│   │   ├── bmtc_static.json
│   │   ├── bmrcl_static.json
│   │   ├── bmtc_fares.json
│   │   └── bmrcl_fares.json
│   └── 📁 live/                 # Live position data
│       ├── bmtc_vehicle_positions.json
│       └── bmrcl_train_positions.json
├── 📁 utils/
│   ├── common.py                # Shared utilities
│   └── error_handler.py         # Error handling and monitoring
├── 📁 logs/                     # Log files (auto-created)
├── demo_interface.py            # Interactive demo
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🎮 Demo Interface Features

The interactive demo provides:

### 1. 🏥 System Health Check
- Kafka connectivity verification
- Data file validation
- Component status monitoring

### 2. 📊 Data Pipeline Management
- Start/stop pipeline components
- Real-time data simulation
- Performance monitoring

### 3. 🗺️ Route Planning Demo
- Interactive location selection
- Multi-criteria route optimization
- Real-time results display

### 4. 📈 Performance Metrics
- Error tracking and analysis
- Operation performance statistics
- Success rate monitoring

### 5. 🔍 Real-time Monitoring
- Live data flow visualization
- Processing status updates
- System activity tracking

## 🧮 Route Optimization

The system provides four optimization strategies:

### 🏃 Fastest Route
- Minimizes total travel time
- Considers real-time delays
- Optimizes transfers

### 💰 Cheapest Route
- Minimizes total cost
- Considers fare structures
- Includes transfer costs

### 🌱 Eco-friendly Route
- Maximizes environmental score
- Prefers electric/metro transport
- Considers carbon footprint

### ⚖️ Balanced Route
- Optimizes composite score
- Balances time, cost, and eco factors
- Provides best overall value

## 📊 Data Formats

### Static Data (GTFS Format)
```json
{
  "agency": "bmtc",
  "data_type": "gtfs_static",
  "timestamp": 1640995200,
  "data": {
    "agency": {...},
    "routes": [...],
    "stops": [...],
    "stop_times": [...],
    "trips": [...],
    "calendar": [...]
  }
}
```

### Live Position Data
```json
{
  "agency": "bmtc",
  "data_type": "vehicle_positions",
  "timestamp": 1640995200,
  "entity": {
    "id": "vehicle_001",
    "vehicle": {
      "trip": {...},
      "position": {...},
      "timestamp": 1640995200
    }
  }
}
```

## 🔧 Configuration

### Kafka Configuration (`config/kafka_config.py`)

```python
# Kafka Broker Settings
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'client_id': 'bangalore_transit_pipeline'
}

# Topic Configuration
KAFKA_TOPICS = {
    'bmtc': {
        'static': 'bmtc.gtfs_static',
        'fares': 'bmtc.fares',
        'positions': 'bmtc.vehicle_positions'
    },
    'bmrcl': {
        'static': 'bmrcl.gtfs_static',
        'fares': 'bmrcl.fares',
        'positions': 'bmrcl.train_positions'
    }
}
```

### Route Optimization Weights

```python
ROUTE_OPTIMIZATION = {
    'time_weight': 0.4,      # 40% weight for time
    'cost_weight': 0.3,      # 30% weight for cost
    'eco_weight': 0.2,       # 20% weight for eco score
    'comfort_weight': 0.1    # 10% weight for comfort
}
```

## 📝 Logging

The system provides comprehensive logging:

- **Application Logs**: `logs/app_*.log`
- **Error Logs**: `logs/errors_*.log`
- **Performance Logs**: `logs/performance_*.log`
- **Route Results**: `logs/route_optimization_results_*.json`

### Log Levels
- `DEBUG`: Detailed debugging information
- `INFO`: General information messages
- `WARNING`: Warning messages
- `ERROR`: Error messages
- `CRITICAL`: Critical error messages

## 🛠️ Troubleshooting

### Common Issues

#### 1. Kafka Connection Failed
```bash
# Check if Kafka is running
brew services list | grep kafka

# Restart Kafka services
brew services restart zookeeper
brew services restart kafka
```

#### 2. Topics Not Found
```bash
# List existing topics
kafka-topics --list --bootstrap-server localhost:9092

# Recreate topics if needed
./scripts/create_topics.sh
```

#### 3. Python Dependencies
```bash
# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Check specific package
pip show kafka-python
```

#### 4. Permission Issues
```bash
# Fix log directory permissions
chmod 755 logs/
```

### Performance Optimization

#### 1. Kafka Performance
- Increase partition count for high-throughput topics
- Adjust batch size and linger time
- Monitor consumer lag

#### 2. Memory Usage
- Limit consumer buffer size
- Implement data cleanup policies
- Monitor heap usage

#### 3. Processing Speed
- Use async processing where possible
- Implement connection pooling
- Cache frequently accessed data

## 🧪 Testing

### Unit Tests
```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_route_optimizer.py
```

### Integration Tests
```bash
# Test Kafka connectivity
python tests/test_kafka_integration.py

# Test end-to-end pipeline
python tests/test_pipeline_integration.py
```

### Load Testing
```bash
# Simulate high message volume
python tests/load_test.py --messages 10000 --rate 100
```

## 📈 Monitoring and Metrics

### Key Metrics
- **Message Throughput**: Messages processed per second
- **Processing Latency**: Time from message receipt to processing
- **Error Rate**: Percentage of failed operations
- **Route Optimization Time**: Time to compute optimal routes

### Health Checks
- Kafka broker connectivity
- Consumer group status
- Data file availability
- System resource usage

## 🔒 Security Considerations

- **Data Privacy**: No personal information in sample data
- **Network Security**: Local Kafka instance only
- **Access Control**: File system permissions
- **Error Handling**: No sensitive data in logs

## 🚀 Future Enhancements

### Planned Features
- **Real API Integration**: Connect to actual BMTC/BMRCL APIs
- **Web Dashboard**: Browser-based monitoring interface
- **Mobile App**: React Native mobile application
- **Machine Learning**: Predictive route optimization
- **Multi-city Support**: Extend to other Indian cities

### Scalability Improvements
- **Kubernetes Deployment**: Container orchestration
- **Distributed Processing**: Multi-node Pathway clusters
- **Data Lake Integration**: Historical data analysis
- **Stream Processing**: Apache Flink integration

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run pre-commit hooks
pre-commit install

# Run code formatting
black .
isort .
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **BMTC** and **BMRCL** for transit data inspiration
- **Apache Kafka** for reliable message streaming
- **Pathway** for real-time data processing
- **GTFS** standard for transit data format

## 📞 Support

For questions or issues:

1. Check the troubleshooting section
2. Review existing issues
3. Create a new issue with detailed information
4. Include logs and error messages

---

**Happy Transit Routing! 🚌🚇**# WayForge-Prototype
