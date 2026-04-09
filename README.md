# OvoMatrix v8.2 - Public Release 🚀

Welcome to **OvoMatrix v8.2**, the ultimate safety-critical poultry telemetry system designed specifically for monitoring Ross 308 broilers. 

This is the production-ready standalone release, designed to interface seamlessly with your physical hardware to provide real-time updates and peace of mind.

## 🌟 What is OvoMatrix?
OvoMatrix is a cutting-edge graphical dashboard and backend orchestrator. It monitors your farm’s environmental metrics, focusing on:
- Temperature
- Humidity
- Ammonia (NH₃)

## 🛠️ Key Features
- **Real-Time Dashboard:** A gorgeous dark-mode GUI to monitor farm health.
- **Hardware Integrations:** Direct Modbus RS-485 reading for physical sensors and GSM Modem (e.g. SIM800L/900) integration for emergency SMS alerts.
- **AI Expert Advisor:** Powered by state-of-the-art LLMs (Gemini, ChatGPT, Claude) to give you contextual advice based on immediate data trends.
- **Advanced Alert Engine:** Built-in hysteresis to categorize the severity of sensor drift (Normal, Warning, Emergency) without alert fatigue.
- **History Hub:** A secure SQLite database recording all metrics and AI responses locally on your machine.

## 🔌 Requirements
- **OS:** Windows PC/Mini-PC (Runs completely locally).
- **Sensors:** Compatible Modbus RS-485 sensors for Temp/Humid/NH3.
- **Modem:** A GSM Modem connected via Serial/USB.
- **MQTT:** Mosquitto MQTT Broker running locally on port `1883`.

## 📥 Getting Started
1. **Run the Application:** Simply double-click on `ovomatrix.exe`.
2. **Configure Ports:** Click on the ⚙️ **Settings** tab. Go to **Hardware** and configure your Modbus and GSM `COM` ports.
3. **Configure Notifications:** Enter comma-separated phone numbers inside the Hardware tab to receive SMS alerts.
4. **Set Up AI Advisor:** Go to the AI Advisor tab in Settings and enter your preferred provider's API key.

## 🐛 Feedback & Bug Reports
Your feedback is extremely valuable as we continuously seek to improve OvoMatrix for real-world farming environments. If you encounter a bug or if you simply have suggestions to improve the user experience:

📧 **Contact:** ellsimohammed8@gmail.com

Thank you for choosing OvoMatrix for your farm's safety and prosperity. 🐔💼
