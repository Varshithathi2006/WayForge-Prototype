// Bangalore Transit Map Application
class BangaloreTransitApp {
    constructor() {
        this.map = null;
        this.sourceMarker = null;
        this.destinationMarker = null;
        this.routeLine = null;
        this.isSelectingSource = true;
        this.selectedTransitType = 'bmtc-ordinary';
        this.liveVehicles = [];
        this.websocket = null;
        this.currentRoute = null;
        this.vehicleMarkers = null;
        this.currentLocationMarker = null;
        this.addressSuggestions = [];
        this.chatbotVisible = false;
        this.usingFallbackTiles = false;
        
        // Enhanced vehicle tracking properties
        this.vehicleTrails = new Map(); // Store vehicle movement history
        this.vehicleUpdateInterval = null;
        this.isLiveTrackingEnabled = true;
        this.selectedVehicleTypes = new Set(['bus', 'metro']);
        this.vehicleAnimations = new Map(); // Store ongoing animations
        this.lastVehiclePositions = new Map(); // Store previous positions for smooth transitions
        
        // Bangalore coordinates
        this.bangaloreCenter = [12.9716, 77.5946];
        
        // Initialize map layers
        this.routeLayer = null;
        this.trafficLayer = null;
        
        this.initializeMap();
        this.setupEventListeners();
        this.connectWebSocket();
        this.loadLiveVehicles();
        this.initializeChatbot();
        this.startLiveVehicleTracking();
    }
    
    initializeMap() {
        // Initialize map centered on Bangalore
        this.map = L.map('map').setView(this.bangaloreCenter, 12);
        
        // Add OpenStreetMap tiles with error handling and fallback
        const primaryTileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors',
            maxZoom: 18,
            errorTileUrl: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjU2IiBoZWlnaHQ9IjI1NiIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMjU2IiBoZWlnaHQ9IjI1NiIgZmlsbD0iI2Y4ZjlmYSIvPjx0ZXh0IHg9IjUwJSIgeT0iNTAlIiBkb21pbmFudC1iYXNlbGluZT0ibWlkZGxlIiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmb250LWZhbWlseT0iQXJpYWwiIGZvbnQtc2l6ZT0iMTQiIGZpbGw9IiM2Yzc1N2QiPk1hcCBUaWxlPC90ZXh0Pjwvc3ZnPg==',
            retryDelay: 1000,
            retryLimit: 3
        });

        // Add fallback tile layer
        const fallbackTileLayer = L.tileLayer('https://cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors, © CartoDB',
            maxZoom: 18,
            subdomains: 'abcd'
        });

        // Try primary layer first, fallback if needed
        primaryTileLayer.on('tileerror', (e) => {
            console.warn('Tile loading error:', e);
            // Switch to fallback after multiple errors
            if (!this.usingFallbackTiles) {
                setTimeout(() => {
                    this.map.removeLayer(primaryTileLayer);
                    fallbackTileLayer.addTo(this.map);
                    this.usingFallbackTiles = true;
                    console.log('Switched to fallback tile layer');
                }, 2000);
            }
        });

        primaryTileLayer.addTo(this.map);
        this.usingFallbackTiles = false;
        
        // Initialize layers
        this.vehicleMarkers = L.layerGroup().addTo(this.map);
        this.routeLayer = L.layerGroup().addTo(this.map);
        this.trafficLayer = L.layerGroup().addTo(this.map);
        this.nearestStopsLayer = L.layerGroup().addTo(this.map);
        this.vehicleTrailsLayer = L.layerGroup().addTo(this.map);
        
        // Define marker icons
        window.sourceIcon = L.divIcon({
            html: '<div class="custom-marker source-marker"><i class="fas fa-map-marker-alt"></i></div>',
            className: 'custom-marker-container',
            iconSize: [30, 30],
            iconAnchor: [15, 30]
        });
        
        // Add click event for location selection
        this.map.on('click', (e) => {
            this.selectLocation(e.latlng);
        });
        
        // Add some Bangalore landmarks for reference
        this.addLandmarks();
    }
    
    addLandmarks() {
        const landmarks = [
            { name: "Majestic Bus Station", coords: [12.9767, 77.5703], icon: "🚌" },
            { name: "Bangalore City Railway Station", coords: [12.9767, 77.5703], icon: "🚂" },
            { name: "Kempegowda International Airport", coords: [13.1986, 77.7066], icon: "✈️" },
            { name: "MG Road Metro Station", coords: [12.9759, 77.6063], icon: "🚇" },
            { name: "Cubbon Park", coords: [12.9698, 77.5936], icon: "🌳" },
            { name: "Electronic City", coords: [12.8456, 77.6603], icon: "💼" },
            { name: "Whitefield", coords: [12.9698, 77.7500], icon: "🏢" },
            { name: "Koramangala", coords: [12.9279, 77.6271], icon: "🏘️" }
        ];
        
        landmarks.forEach(landmark => {
            L.marker(landmark.coords)
                .addTo(this.map)
                .bindPopup(`${landmark.icon} ${landmark.name}`)
                .openPopup();
        });
    }
    
    setupEventListeners() {
        // Transit type selection
        document.querySelectorAll('.transit-type').forEach(element => {
            element.addEventListener('click', () => {
                document.querySelectorAll('.transit-type').forEach(el => el.classList.remove('selected'));
                element.classList.add('selected');
                this.selectedTransitType = element.dataset.type;
                this.calculatePricing();
                this.showTransportDetails();
            });
        });
        
        // Set default selection
        document.querySelector('.transit-type').classList.add('selected');
        
        // Location input fields with address suggestions
        this.setupAddressSuggestions('source');
        this.setupAddressSuggestions('destination');
        
        // Current location button
        const currentLocationBtn = document.getElementById('current-location-btn');
        if (currentLocationBtn) {
            currentLocationBtn.addEventListener('click', () => {
                this.getCurrentLocation();
            });
        }
        
        // Chatbot toggle
        const chatbotToggle = document.getElementById('chatbot-toggle');
        if (chatbotToggle) {
            chatbotToggle.addEventListener('click', () => {
                this.toggleChatbot();
            });
        }
        
        // AI recommendations button
        const aiRecommendationsBtn = document.getElementById('get-ai-recommendations');
        if (aiRecommendationsBtn) {
            aiRecommendationsBtn.addEventListener('click', () => {
                this.getLLMRecommendations();
            });
        }
    }
    
    selectLocation(latlng) {
        if (this.isSelectingSource) {
            this.setSourceLocation(latlng);
        } else {
            this.setDestinationLocation(latlng);
        }
    }
    
    async setSourceLocation(latlng, name = null) {
        if (this.sourceMarker) {
            this.map.removeLayer(this.sourceMarker);
        }
        
        this.sourceMarker = L.marker(latlng, { icon: sourceIcon }).addTo(this.map);
        
        // Get location name if not provided
        if (!name) {
            name = await this.reverseGeocode(latlng, 'source');
        }
        
        // Set the sourceLocation property for pricing calculations
        this.sourceLocation = {
            lat: latlng.lat,
            lng: latlng.lng,
            name: name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`
        };
        
        document.getElementById('source').value = name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
        this.isSelectingSource = false;
        
        // Find and display nearest stops
        await this.findAndDisplayNearestStops(latlng, 'source');
        
        // Calculate route if destination is set
        if (this.destinationMarker) {
            await this.calculateEnhancedRoute();
        }
        
        this.showNotification('Source location set! Click on map to set destination.', 'success');
    }
    
    async setDestinationLocation(latlng, name = null) {
        if (this.destinationMarker) {
            this.map.removeLayer(this.destinationMarker);
        }
        
        // Create enhanced destination marker
        const destIcon = L.divIcon({
            html: '<div class="custom-marker destination-marker"><i class="fas fa-flag-checkered"></i></div>',
            className: 'custom-marker-container',
            iconSize: [30, 30],
            iconAnchor: [15, 30]
        });
        
        this.destinationMarker = L.marker(latlng, { icon: destIcon }).addTo(this.map);
        
        // Get location name if not provided
        if (!name) {
            name = await this.reverseGeocode(latlng, 'destination');
        }
        
        // Set the destinationLocation property for pricing calculations
        this.destinationLocation = {
            lat: latlng.lat,
            lng: latlng.lng,
            name: name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`
        };
        
        document.getElementById('destination').value = name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
        this.isSelectingSource = true;
        
        // Find and display nearest stops
        await this.findAndDisplayNearestStops(latlng, 'destination');
        
        // Calculate route if source is set
        if (this.sourceMarker) {
            await this.calculateEnhancedRoute();
        }
        
        this.showNotification('Destination location set! Calculating route...', 'success');
    }
    
    async findAndDisplayNearestStops(latlng, type) {
        try {
            const response = await fetch('/api/nearest-stops', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    latitude: latlng.lat,
                    longitude: latlng.lng,
                    max_distance_km: 1.0
                })
            });
            
            if (response.ok) {
                const nearestStopsResponse = await response.json();
                // Extract the nearest_stops array from the response
                const nearestStops = nearestStopsResponse.nearest_stops || [];
                this.displayNearestStops(nearestStops, type);
            }
        } catch (error) {
            console.error('Error finding nearest stops:', error);
        }
    }
    
    displayNearestStops(stops, type) {
        // Clear previous nearest stops for this type
        this.nearestStopsLayer.eachLayer(layer => {
            if (layer.options.stopType === type) {
                this.nearestStopsLayer.removeLayer(layer);
            }
        });
        
        // Ensure stops is an array before processing
        if (!Array.isArray(stops)) {
            console.warn('Expected stops to be an array, received:', typeof stops);
            return;
        }
        
        stops.forEach(stop => {
            const stopIcon = L.divIcon({
                html: `<div class="nearest-stop-marker ${stop.stop_type}">
                        <i class="fas ${stop.stop_type === 'bus_stop' ? 'fa-bus' : 'fa-subway'}"></i>
                       </div>`,
                className: 'nearest-stop-container',
                iconSize: [20, 20],
                iconAnchor: [10, 10]
            });
            
            const marker = L.marker([stop.latitude, stop.longitude], { 
                icon: stopIcon,
                stopType: type
            }).addTo(this.nearestStopsLayer);
            
            marker.bindPopup(`
                <div class="stop-popup">
                    <h4>${stop.stop_name}</h4>
                    <p><strong>Type:</strong> ${stop.stop_type.replace('_', ' ').toUpperCase()}</p>
                    <p><strong>Distance:</strong> ${stop.distance_meters}m</p>
                    <p><strong>Walking Time:</strong> ${stop.walking_time_minutes} min</p>
                    <button onclick="app.useNearestStop('${stop.stop_id}', ${stop.latitude}, ${stop.longitude}, '${stop.stop_name}', '${type}')">
                        Use This Stop
                    </button>
                </div>
            `);
        });
    }
    
    useNearestStop(stopId, lat, lng, name, type) {
        if (type === 'source') {
            this.setSourceLocation(L.latLng(lat, lng), name);
        } else {
            this.setDestinationLocation(L.latLng(lat, lng), name);
        }
        this.map.closePopup();
    }
    
    async calculateEnhancedRoute() {
        console.log('🚀 Starting calculateEnhancedRoute...');
        
        if (!this.sourceMarker || !this.destinationMarker) {
            console.log('❌ Missing source or destination marker');
            return;
        }
        
        const sourceLatLng = this.sourceMarker.getLatLng();
        const destLatLng = this.destinationMarker.getLatLng();
        
        console.log('📍 Source:', sourceLatLng);
        console.log('📍 Destination:', destLatLng);
        console.log('🚌 Transport mode:', this.selectedTransitType);
        
        try {
            this.showNotification('Calculating enhanced route with real-time data...', 'info');
            
            const requestBody = {
                source: {
                    latitude: sourceLatLng.lat,
                    longitude: sourceLatLng.lng,
                    name: document.getElementById('source').value
                },
                destination: {
                    latitude: destLatLng.lat,
                    longitude: destLatLng.lng,
                    name: document.getElementById('destination').value
                },
                transport_mode: this.selectedTransitType,
                include_real_time: this.realTimeEnabled || true,
                show_alternatives: this.showAlternativeRoutes || false
            };
            
            console.log('📤 Sending request to /api/enhanced-route:', requestBody);
            
            const response = await fetch('/api/enhanced-route', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            });
            
            console.log('📥 Response status:', response.status, response.statusText);
            
            if (response.ok) {
                const routeData = await response.json();
                console.log('✅ Received route data:', routeData);
                console.log('📏 Total distance from API:', routeData.route?.total_distance_km);
                console.log('⏱️ Total duration from API:', routeData.route?.total_duration_minutes);
                console.log('🛣️ Route segments:', routeData.route?.segments);
                
                this.displayEnhancedRoute(routeData);
                this.displayEnhancedResults(routeData);
                
                // Update transport details with route-specific information
                await this.calculatePricing();
            } else {
                const errorText = await response.text();
                console.error('❌ API Error:', response.status, errorText);
                throw new Error(`Failed to calculate route: ${response.status} ${errorText}`);
            }
        } catch (error) {
            console.error('💥 Error calculating enhanced route:', error);
            this.showNotification('Error calculating route. Using fallback method.', 'warning');
            console.log('🔄 Falling back to Haversine calculation...');
            this.calculateFallbackRoute();
        }
    }
    
    displayEnhancedRoute(routeData) {
        // Clear previous route
        this.routeLayer.clearLayers();
        
        if (!routeData.route || !routeData.route.geometry) {
            console.warn('No route data or geometry available');
            return;
        }
        
        const route = routeData.route;
        
        // Create main route line with traffic-aware styling
        const routeCoords = route.geometry.map(coord => [coord[0], coord[1]]);
        
        // Check if segments exist and are properly structured
        if (route.segments && Array.isArray(route.segments) && route.segments.length > 0) {
            // Create route segments with different colors based on traffic
            route.segments.forEach((segment, index) => {
                if (segment.geometry && segment.geometry.length > 0) {
                    const segmentCoords = segment.geometry.map(coord => [coord[0], coord[1]]);
                    
                    // Determine color based on traffic delay
                    let color = '#2E8B57'; // Default green
                    const trafficDelay = segment.traffic_delay_minutes || 0;
                    if (trafficDelay > 10) {
                        color = '#FF4500'; // Red for heavy traffic
                    } else if (trafficDelay > 5) {
                        color = '#FFA500'; // Orange for moderate traffic
                    } else if (trafficDelay > 0) {
                        color = '#FFD700'; // Yellow for light traffic
                    }
                    
                    const segmentLine = L.polyline(segmentCoords, {
                        color: color,
                        weight: 6,
                        opacity: 0.8,
                        smoothFactor: 1
                    }).addTo(this.routeLayer);
                    
                    // Add popup with segment details
                    segmentLine.bindPopup(`
                        <div class="segment-popup">
                            <h4>Route Segment ${index + 1}</h4>
                            <p><strong>Distance:</strong> ${(segment.distance_km || 0).toFixed(2)} km</p>
                            <p><strong>Duration:</strong> ${(segment.duration_minutes || 0).toFixed(1)} min</p>
                            <p><strong>Traffic Delay:</strong> ${trafficDelay} min</p>
                            <p><strong>Instructions:</strong> ${segment.instructions || 'Continue on route'}</p>
                        </div>
                    `);
                }
            });
        } else {
            // Fallback: Display the main route as a single line when segments are not available
            console.log('No segments available, displaying main route line');
            const mainRouteLine = L.polyline(routeCoords, {
                color: '#2E8B57',
                weight: 6,
                opacity: 0.8,
                smoothFactor: 1
            }).addTo(this.routeLayer);
            
            // Add popup with route details
            mainRouteLine.bindPopup(`
                <div class="route-popup">
                    <h4>Route</h4>
                    <p><strong>Distance:</strong> ${(route.total_distance_km || 0).toFixed(2)} km</p>
                    <p><strong>Duration:</strong> ${(route.total_duration_minutes || 0).toFixed(1)} min</p>
                    <p><strong>Transport:</strong> ${route.transport_mode || 'Unknown'}</p>
                </div>
            `);
        }
        
        // Add route markers for key points
        if (route.segments.length > 0) {
            route.segments.forEach((segment, index) => {
                if (segment.real_time_data && segment.real_time_data.traffic) {
                    segment.real_time_data.traffic.forEach(traffic => {
                        if (traffic.congestion_level === 'high' || traffic.congestion_level === 'severe') {
                            const trafficIcon = L.divIcon({
                                html: '<div class="traffic-marker"><i class="fas fa-exclamation-triangle"></i></div>',
                                className: 'traffic-marker-container',
                                iconSize: [20, 20],
                                iconAnchor: [10, 10]
                            });
                            
                            // Use segment midpoint for traffic marker
                            if (segment.geometry && segment.geometry.length > 0) {
                                const midIndex = Math.floor(segment.geometry.length / 2);
                                const midPoint = segment.geometry[midIndex];
                                
                                L.marker([midPoint[0], midPoint[1]], { icon: trafficIcon })
                                    .addTo(this.trafficLayer)
                                    .bindPopup(`
                                        <div class="traffic-popup">
                                            <h4>Traffic Alert</h4>
                                            <p><strong>Congestion:</strong> ${traffic.congestion_level}</p>
                                            <p><strong>Delay:</strong> ${traffic.estimated_delay_minutes} min</p>
                                            <p><strong>Road:</strong> ${traffic.road_name}</p>
                                        </div>
                                    `);
                            }
                        }
                    });
                }
            });
        }
        
        // Fit map to route bounds
        if (routeCoords.length > 0) {
            const routeBounds = L.latLngBounds(routeCoords);
            this.map.fitBounds(routeBounds, { padding: [20, 20] });
        }
        
        this.currentRoute = routeData;
    }
    
    displayEnhancedResults(routeData) {
        const resultsContainer = document.getElementById('results');
        if (!resultsContainer) return;
        
        const route = routeData.route;
        const totalTrafficDelay = route.segments.reduce((sum, seg) => sum + (seg.traffic_delay_minutes || 0), 0);
        
        resultsContainer.innerHTML = `
            <div class="enhanced-results">
                <div class="route-summary">
                    <h3>Journey Details - ${route.transport_mode}</h3>
                    <div class="summary-grid">
                        <div class="summary-item">
                            <i class="fas fa-route"></i>
                            <span class="label">Distance</span>
                            <span class="value">${route.total_distance_km.toFixed(2)} km</span>
                        </div>
                        <div class="summary-item">
                            <i class="fas fa-clock"></i>
                            <span class="label">Duration</span>
                            <span class="value">${route.total_duration_minutes.toFixed(1)} min</span>
                        </div>
                        <div class="summary-item">
                            <i class="fas fa-rupee-sign"></i>
                            <span class="label">Estimated Fare</span>
                            <span class="value">₹${route.estimated_fare || 'N/A'}</span>
                        </div>
                        <div class="summary-item">
                            <i class="fas fa-traffic-light"></i>
                            <span class="label">Traffic Delay</span>
                            <span class="value ${totalTrafficDelay > 0 ? 'delay' : ''}">${totalTrafficDelay} min</span>
                        </div>
                        <div class="summary-item">
                            <i class="fas fa-star"></i>
                            <span class="label">Comfort Level</span>
                            <span class="value">${this.getComfortLevel(route.transport_mode)}</span>
                        </div>
                        <div class="summary-item">
                            <i class="fas fa-leaf"></i>
                            <span class="label">Eco-Friendly</span>
                            <span class="value">${this.getEcoRating(route.transport_mode)}</span>
                        </div>
                    </div>
                </div>
                
                ${this.renderTransportModeDetails(route)}
                
                ${route.nearest_stops_info ? this.renderNearestStopsInfo(route.nearest_stops_info) : ''}
                
                <div class="route-segments">
                    <h4>Step-by-Step Directions</h4>
                    ${route.segments.map((segment, index) => `
                        <div class="segment-card">
                            <div class="segment-header">
                                <span class="segment-number">${index + 1}</span>
                                <span class="segment-distance">${segment.distance_km.toFixed(2)} km</span>
                                <span class="segment-duration">${segment.duration_minutes.toFixed(1)} min</span>
                                ${segment.traffic_delay_minutes > 0 ? 
                                    `<span class="traffic-delay">+${segment.traffic_delay_minutes} min delay</span>` : ''}
                            </div>
                            <div class="segment-instructions">${segment.instructions}</div>
                            ${segment.real_time_data ? this.renderRealTimeData(segment.real_time_data) : ''}
                        </div>
                    `).join('')}
                </div>
                
                ${routeData.alternatives ? this.renderAlternativeRoutes(routeData.alternatives) : ''}
            </div>
        `;
    }
    
    renderTransportModeDetails(route) {
        const mode = route.transport_mode;
        let details = '';
        
        switch(mode) {
            case 'BMTC Bus':
                details = `
                    <div class="transport-details">
                        <h4><i class="fas fa-bus"></i> BMTC Bus Details</h4>
                        <div class="details-grid">
                            <div class="detail-item">
                                <span class="label">Bus Type:</span>
                                <span class="value">Regular/AC Bus</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Frequency:</span>
                                <span class="value">Every 10-15 minutes</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Operating Hours:</span>
                                <span class="value">5:00 AM - 11:00 PM</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Payment:</span>
                                <span class="value">Cash, Card, UPI</span>
                            </div>
                        </div>
                    </div>
                `;
                break;
            case 'Metro':
                details = `
                    <div class="transport-details">
                        <h4><i class="fas fa-subway"></i> Namma Metro Details</h4>
                        <div class="details-grid">
                            <div class="detail-item">
                                <span class="label">Line:</span>
                                <span class="value">Purple/Green Line</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Frequency:</span>
                                <span class="value">Every 3-5 minutes</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Operating Hours:</span>
                                <span class="value">5:00 AM - 11:00 PM</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Payment:</span>
                                <span class="value">Metro Card, Token, UPI</span>
                            </div>
                        </div>
                    </div>
                `;
                break;
            case 'Taxi':
                details = `
                    <div class="transport-details">
                        <h4><i class="fas fa-taxi"></i> Taxi Details</h4>
                        <div class="details-grid">
                            <div class="detail-item">
                                <span class="label">Service:</span>
                                <span class="value">Ola, Uber, Local Taxi</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Availability:</span>
                                <span class="value">24/7</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Booking:</span>
                                <span class="value">App-based/On-demand</span>
                            </div>
                            <div class="detail-item">
                                <span class="label">Payment:</span>
                                <span class="value">Cash, Card, UPI, Wallet</span>
                            </div>
                        </div>
                    </div>
                `;
                break;
            default:
                details = `
                    <div class="transport-details">
                        <h4><i class="fas fa-route"></i> Route Details</h4>
                        <p>Mixed transportation mode journey with multiple options.</p>
                    </div>
                `;
        }
        
        return details;
    }
    
    getComfortLevel(mode) {
        switch(mode) {
            case 'Metro': return 'High';
            case 'AC Bus': return 'High';
            case 'Taxi': return 'Very High';
            case 'BMTC Bus': return 'Medium';
            default: return 'Medium';
        }
    }
    
    getEcoRating(mode) {
        switch(mode) {
            case 'Metro': return 'Excellent';
            case 'BMTC Bus': return 'Good';
            case 'AC Bus': return 'Good';
            case 'Taxi': return 'Fair';
            default: return 'Good';
        }
    }
    
    renderNearestStopsInfo(stopsInfo) {
        if (!stopsInfo.source_stops.length && !stopsInfo.destination_stops.length) {
            return '';
        }
        
        return `
            <div class="nearest-stops-info">
                <h4>Nearby Transit Stops</h4>
                ${stopsInfo.source_stops.length > 0 ? `
                    <div class="stops-section">
                        <h5>Near Source</h5>
                        <div class="stops-list">
                            ${stopsInfo.source_stops.slice(0, 3).map(stop => `
                                <div class="stop-item">
                                    <i class="fas ${stop.stop_type === 'bus_stop' ? 'fa-bus' : 'fa-subway'}"></i>
                                    <span class="stop-name">${stop.stop_name}</span>
                                    <span class="stop-distance">${stop.distance_meters}m</span>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                ` : ''}
                ${stopsInfo.destination_stops.length > 0 ? `
                    <div class="stops-section">
                        <h5>Near Destination</h5>
                        <div class="stops-list">
                            ${stopsInfo.destination_stops.slice(0, 3).map(stop => `
                                <div class="stop-item">
                                    <i class="fas ${stop.stop_type === 'bus_stop' ? 'fa-bus' : 'fa-subway'}"></i>
                                    <span class="stop-name">${stop.stop_name}</span>
                                    <span class="stop-distance">${stop.distance_meters}m</span>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                ` : ''}
            </div>
        `;
    }
    
    renderRealTimeData(realTimeData) {
        let html = '<div class="real-time-data">';
        
        if (realTimeData.traffic && realTimeData.traffic.length > 0) {
            html += '<div class="traffic-info"><h6>Traffic Conditions</h6>';
            realTimeData.traffic.forEach(traffic => {
                html += `
                    <div class="traffic-item ${traffic.congestion_level}">
                        <span class="road-name">${traffic.road_name}</span>
                        <span class="congestion-level">${traffic.congestion_level}</span>
                    </div>
                `;
            });
            html += '</div>';
        }
        
        if (realTimeData.buses && realTimeData.buses.length > 0) {
            html += '<div class="bus-info"><h6>Nearby Buses</h6>';
            realTimeData.buses.slice(0, 3).forEach(bus => {
                html += `
                    <div class="bus-item">
                        <span class="route-number">${bus.route_number}</span>
                        <span class="arrival-time">${bus.estimated_arrival_minutes} min</span>
                    </div>
                `;
            });
            html += '</div>';
        }
        
        if (realTimeData.taxis && realTimeData.taxis.length > 0) {
            html += '<div class="taxi-info"><h6>Available Taxis</h6>';
            realTimeData.taxis.slice(0, 2).forEach(taxi => {
                html += `
                    <div class="taxi-item">
                        <span class="service-name">${taxi.service_name}</span>
                        <span class="fare-estimate">₹${taxi.estimated_fare}</span>
                        <span class="eta">${taxi.eta_minutes} min</span>
                    </div>
                `;
            });
            html += '</div>';
        }
        
        html += '</div>';
        return html;
    }
    
    renderAlternativeRoutes(alternatives) {
        if (!alternatives || alternatives.length === 0) {
            return '';
        }
        
        let html = `
            <div class="alternative-routes">
                <h4><i class="fas fa-route"></i> Alternative Routes</h4>
                <div class="alternatives-grid">
        `;
        
        alternatives.forEach((alt, index) => {
            html += `
                <div class="alternative-card">
                    <div class="alt-header">
                        <span class="alt-number">Route ${index + 1}</span>
                        <span class="alt-duration">${alt.duration_minutes ? alt.duration_minutes.toFixed(1) : 'N/A'} min</span>
                    </div>
                    <div class="alt-details">
                        <div class="alt-distance">
                            <i class="fas fa-road"></i>
                            ${alt.distance_km ? alt.distance_km.toFixed(2) : 'N/A'} km
                        </div>
                        <div class="alt-mode">
                            <i class="fas ${this.getTransportIcon(alt.transport_mode)}"></i>
                            ${alt.transport_mode || 'Mixed'}
                        </div>
                        ${alt.fare ? `
                            <div class="alt-fare">
                                <i class="fas fa-rupee-sign"></i>
                                ₹${alt.fare.toFixed(2)}
                            </div>
                        ` : ''}
                    </div>
                    ${alt.traffic_delay_minutes > 0 ? `
                        <div class="alt-delay">
                            <i class="fas fa-exclamation-triangle"></i>
                            +${alt.traffic_delay_minutes} min delay
                        </div>
                    ` : ''}
                </div>
            `;
        });
        
        html += `
                </div>
            </div>
        `;
        
        return html;
    }
    
    initializeRealTimeUpdates() {
        // Update real-time data every 30 seconds
        setInterval(() => {
            if (this.currentRoute && this.realTimeEnabled) {
                this.updateRealTimeData();
            }
        }, 30000);
    }
    
    async updateRealTimeData() {
        if (!this.currentRoute) return;
        
        try {
            const response = await fetch('/api/real-time-update', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    route_id: this.currentRoute.route_id || 'current',
                    segments: this.currentRoute.route.segments.map(seg => ({
                        geometry: seg.geometry
                    }))
                })
            });
            
            if (response.ok) {
                const updateData = await response.json();
                this.handleRealTimeUpdate(updateData);
            }
        } catch (error) {
            console.error('Error updating real-time data:', error);
        }
    }

    async reverseGeocode(latlng, type) {
        try {
            const response = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${latlng.lat}&lon=${latlng.lng}&zoom=18&addressdetails=1`);
            const data = await response.json();
            return data.display_name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
        } catch (error) {
            console.error('Reverse geocoding error:', error);
            return `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
        }
    }

    async searchLocationEnhanced(query, type) {
        if (query.length < 3) return;
        
        try {
            // Search both OSM and local transit stops
            const [osmResults, transitResults] = await Promise.all([
                this.searchOSM(query),
                this.searchTransitStops(query)
            ]);
            
            const allResults = [...osmResults, ...transitResults];
            this.displaySearchResults(allResults, type);
        } catch (error) {
            console.error('Enhanced search error:', error);
        }
    }
    
    async searchOSM(query) {
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query + ', Bangalore')}&limit=5`);
        const data = await response.json();
        return data.map(item => ({
            name: item.display_name,
            lat: parseFloat(item.lat),
            lng: parseFloat(item.lon),
            type: 'location'
        }));
    }
    
    async searchTransitStops(query) {
        try {
            const response = await fetch('/api/search-stops', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: query })
            });
            
            if (response.ok) {
                const data = await response.json();
                return data.map(stop => ({
                    name: stop.stop_name,
                    lat: stop.latitude,
                    lng: stop.longitude,
                    type: stop.stop_type
                }));
            }
        } catch (error) {
            console.error('Transit stop search error:', error);
        }
        return [];
    }

    calculateFallbackRoute() {
        console.log('🔄 FALLBACK: Using Haversine calculation...');
        
        if (!this.sourceMarker || !this.destinationMarker) return;
        
        const sourceLatLng = this.sourceMarker.getLatLng();
        const destLatLng = this.destinationMarker.getLatLng();
        
        console.log('📍 FALLBACK Source:', sourceLatLng);
        console.log('📍 FALLBACK Destination:', destLatLng);
        
        // Simple straight line route as fallback
        const routeCoords = [
            [sourceLatLng.lat, sourceLatLng.lng],
            [destLatLng.lat, destLatLng.lng]
        ];
        
        this.routeLayer.clearLayers();
        
        const fallbackRoute = L.polyline(routeCoords, {
            color: '#666',
            weight: 4,
            opacity: 0.7,
            dashArray: '10, 10'
        }).addTo(this.routeLayer);
        
        const distance = this.calculateDistance(
            sourceLatLng.lat, sourceLatLng.lng,
            destLatLng.lat, destLatLng.lng
        );
        
        console.log('📏 FALLBACK Distance calculated:', distance.toFixed(2), 'km');
        
        fallbackRoute.bindPopup(`
            <div class="fallback-route-popup">
                <h4>Fallback Route</h4>
                <p>Approximate distance: ${distance.toFixed(2)} km</p>
                <p>This is a straight-line estimate.</p>
            </div>
        `);
        
        this.map.fitBounds(fallbackRoute.getBounds(), { padding: [20, 20] });
    }

    calculateDistance(lat1, lon1, lat2, lon2) {
        const R = 6371; // Earth's radius in kilometers
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLon = (lon2 - lon1) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLon/2) * Math.sin(dLon/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c;
    }

    showNotification(message, type = 'info', duration = 5000) {
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas ${type === 'success' ? 'fa-check-circle' : 
                              type === 'warning' ? 'fa-exclamation-triangle' : 
                              type === 'error' ? 'fa-times-circle' : 'fa-info-circle'}"></i>
                <span>${message}</span>
                <button class="notification-close" onclick="this.parentElement.parentElement.remove()">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;
        
        document.body.appendChild(notification);
        
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, duration);
    }

    displayNearbyVehicles(vehicles) {
        // Display nearby vehicles on the map
        if (vehicles && vehicles.length > 0) {
            this.updateVehiclePositions(vehicles);
            this.showNotification(`Found ${vehicles.length} nearby vehicles`, 'info');
        }
    }
    
    handleRouteResponse(route) {
        // Handle route response from WebSocket
        if (route && route.coordinates) {
            console.log('Received route data:', route);
            // Could be used to update route display in the future
        }
    }
    
    // Helper functions to format values and handle NaN
    formatDistance(distance) {
        if (!distance || isNaN(distance) || distance <= 0) {
            return '0.0';
        }
        return distance.toFixed(1);
    }
    
    formatTime(time) {
        if (!time || isNaN(time) || time <= 0) {
            return 'N/A';
        }
        return time.toFixed(0);
    }
    
    formatFare(fare) {
        if (!fare || isNaN(fare) || fare < 0) {
            return '0';
        }
        return Math.round(fare);
    }

    connectWebSocket() {
        const ports = [8766, 8767]; // Try both ports
        let currentPortIndex = this.websocketPortIndex || 0;
        
        const tryConnection = (portIndex) => {
            if (portIndex >= ports.length) {
                console.error('All WebSocket ports failed');
                this.showNotification('Real-time updates unavailable', 'error');
                // Retry from the beginning after 10 seconds
                setTimeout(() => {
                    this.websocketPortIndex = 0;
                    this.connectWebSocket();
                }, 10000);
                return;
            }
            
            const port = ports[portIndex];
            console.log(`Attempting WebSocket connection on port ${port}`);
            
            try {
                this.websocket = new WebSocket(`ws://localhost:${port}`);
                
                this.websocket.onopen = () => {
                    console.log(`WebSocket connected on port ${port}`);
                    this.websocketPortIndex = portIndex; // Remember successful port
                    this.showNotification('Real-time updates connected', 'success');
                };
                
                this.websocket.onmessage = (event) => {
                    try {
                        const data = JSON.parse(event.data);
                        this.handleRealTimeUpdate(data);
                    } catch (error) {
                        console.error('Error parsing WebSocket message:', error);
                    }
                };
                
                this.websocket.onclose = () => {
                    console.log('WebSocket disconnected');
                    this.showNotification('Real-time updates disconnected', 'warning');
                    
                    // Attempt to reconnect after 5 seconds
                    setTimeout(() => {
                        this.connectWebSocket();
                    }, 5000);
                };
                
                this.websocket.onerror = (error) => {
                    console.error(`WebSocket error on port ${port}:`, error);
                    // Try next port
                    setTimeout(() => {
                        tryConnection(portIndex + 1);
                    }, 1000);
                };
            } catch (error) {
                console.error(`Failed to connect WebSocket on port ${port}:`, error);
                // Try next port
                setTimeout(() => {
                    tryConnection(portIndex + 1);
                }, 1000);
            }
        };
        
        tryConnection(currentPortIndex);
    }

    handleRealTimeUpdate(data) {
        switch (data.type) {
            case 'connection_established':
                console.log('WebSocket connection established');
                break;
                
            case 'realtime_update':
                console.log('Received real-time update:', data);
                break;
                
            case 'vehicle_positions':
                this.updateVehiclePositions(data.vehicles);
                break;
                
            case 'traffic_update':
                this.updateTrafficConditions(data.traffic);
                break;
                
            case 'route_update':
                if (this.currentRoute) {
                    this.calculateEnhancedRoute();
                }
                break;
                
            default:
                console.log('Unknown WebSocket message type:', data.type);
        }
    }

    updateVehiclePositions(vehicles) {
        if (!this.vehicleMarkers) {
            this.vehicleMarkers = L.layerGroup().addTo(this.map);
        }

        // Filter vehicles based on selected types
        const filteredVehicles = vehicles.filter(vehicle => 
            this.selectedVehicleTypes.has(vehicle.type)
        );

        // Clear existing markers
        this.vehicleMarkers.clearLayers();

        // Add enhanced vehicle markers with animations
        filteredVehicles.forEach(vehicle => {
            const vehicleId = vehicle.id || `${vehicle.type}_${vehicle.route_name}_${vehicle.lat}_${vehicle.lng}`;
            const currentPos = [vehicle.lat, vehicle.lng];
            const lastPos = this.lastVehiclePositions.get(vehicleId);

            // Create enhanced vehicle icon with status indicators
            const occupancyColor = this.getOccupancyColor(vehicle.occupancy);
            const statusIcon = this.getStatusIcon(vehicle.status);
            
            const icon = L.divIcon({
                className: `vehicle-marker enhanced-vehicle-marker ${vehicle.type}`,
                html: `
                    <div class="vehicle-icon-container">
                        <div class="vehicle-icon ${vehicle.type}" style="border-color: ${occupancyColor}">
                            ${vehicle.type === 'bus' ? '🚌' : vehicle.type === 'metro' ? '🚇' : '🚕'}
                        </div>
                        <div class="vehicle-status">${statusIcon}</div>
                        ${vehicle.speed ? `<div class="vehicle-speed">${Math.round(vehicle.speed)}km/h</div>` : ''}
                    </div>
                `,
                iconSize: [40, 40],
                iconAnchor: [20, 20]
            });

            // Create marker with enhanced popup
            const marker = L.marker(currentPos, { icon })
                .bindPopup(this.createEnhancedVehiclePopup(vehicle));

            // Add vehicle trail if we have previous position
            if (lastPos && this.isLiveTrackingEnabled) {
                this.addVehicleTrail(vehicleId, lastPos, currentPos, vehicle.type);
            }

            // Store current position for next update
            this.lastVehiclePositions.set(vehicleId, currentPos);

            // Add click event for vehicle tracking
            marker.on('click', () => {
                this.trackVehicle(vehicle);
            });

            this.vehicleMarkers.addLayer(marker);

            // Add smooth animation if vehicle moved
            if (lastPos && this.calculateDistance(lastPos[0], lastPos[1], currentPos[0], currentPos[1]) > 0.001) {
                this.animateVehicleMovement(marker, lastPos, currentPos);
            }
        });

        // Update vehicle count display
        this.updateVehicleCountDisplay(filteredVehicles.length);
    }

    updateTrafficConditions(trafficData) {
        // Update traffic layer if it exists
        if (this.trafficLayer) {
            this.trafficLayer.clearLayers();
        } else {
            this.trafficLayer = L.layerGroup().addTo(this.map);
        }

        trafficData.forEach(traffic => {
            const color = this.getTrafficColor(traffic.congestion_level);
            const polyline = L.polyline(traffic.coordinates, {
                color: color,
                weight: 6,
                opacity: 0.7
            }).bindPopup(`
                <div class="traffic-popup">
                    <h4>Traffic Condition</h4>
                    <p><strong>Level:</strong> ${traffic.congestion_level}</p>
                    <p><strong>Speed:</strong> ${traffic.average_speed} km/h</p>
                    <p><strong>Delay:</strong> ${traffic.delay_minutes} min</p>
                </div>
            `);

            this.trafficLayer.addLayer(polyline);
        });
    }

    getTrafficColor(congestionLevel) {
        switch (congestionLevel) {
            case 'heavy': return '#ff0000';
            case 'moderate': return '#ff8800';
            case 'light': return '#ffff00';
            default: return '#00ff00';
        }
    }

    loadLiveVehicles() {
        // Initialize vehicle markers layer if not exists
        if (!this.vehicleMarkers) {
            this.vehicleMarkers = L.layerGroup().addTo(this.map);
        }
        
        // Load live vehicles from API
        Promise.all([
            fetch('/api/live/bmtc').then(r => r.json()),
            fetch('/api/live/bmrcl').then(r => r.json())
        ]).then(([bmtcData, bmrclData]) => {
            const allVehicles = [
                ...(bmtcData.data?.entity || []).map(v => ({
                    ...v.vehicle,
                    lat: v.vehicle?.position?.latitude,
                    lng: v.vehicle?.position?.longitude,
                    type: 'bus',
                    route_name: v.vehicle?.trip?.route_id || 'Unknown Route',
                    occupancy: v.vehicle?.occupancy_status || 'Unknown'
                })),
                ...(bmrclData.data?.entity || []).map(v => ({
                    ...v.vehicle,
                    lat: v.vehicle?.position?.latitude,
                    lng: v.vehicle?.position?.longitude,
                    type: 'metro',
                    route_name: v.vehicle?.trip?.route_id || 'Unknown Route',
                    occupancy: v.vehicle?.occupancy_status || 'Unknown'
                }))
            ];
            this.updateVehiclePositions(allVehicles);
        }).catch(error => {
            console.error('Error loading live vehicles:', error);
        });
    }

    // Enhanced vehicle tracking helper methods
    getOccupancyColor(occupancy) {
        switch (occupancy?.toLowerCase()) {
            case 'empty': return '#4CAF50';
            case 'many_seats_available': return '#8BC34A';
            case 'few_seats_available': return '#FFC107';
            case 'standing_room_only': return '#FF9800';
            case 'crushed_standing_room_only': return '#F44336';
            case 'full': return '#D32F2F';
            default: return '#2196F3';
        }
    }

    getStatusIcon(status) {
        switch (status?.toLowerCase()) {
            case 'active': return '🟢';
            case 'delayed': return '🟡';
            case 'stopped': return '🔴';
            case 'maintenance': return '🔧';
            default: return '🟢';
        }
    }

    createEnhancedVehiclePopup(vehicle) {
        const lastUpdate = new Date().toLocaleTimeString();
        return `
            <div class="enhanced-vehicle-popup">
                <div class="popup-header">
                    <h4>${vehicle.route_name || 'Unknown Route'}</h4>
                    <span class="vehicle-type-badge ${vehicle.type}">${vehicle.type.toUpperCase()}</span>
                </div>
                <div class="popup-content">
                    <div class="info-row">
                        <span class="label">Status:</span>
                        <span class="value">${this.getStatusIcon(vehicle.status)} ${vehicle.status || 'Active'}</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Occupancy:</span>
                        <span class="value occupancy-${vehicle.occupancy?.toLowerCase()}">${vehicle.occupancy || 'Unknown'}</span>
                    </div>
                    ${vehicle.speed ? `
                        <div class="info-row">
                            <span class="label">Speed:</span>
                            <span class="value">${Math.round(vehicle.speed)} km/h</span>
                        </div>
                    ` : ''}
                    ${vehicle.delay_minutes ? `
                        <div class="info-row">
                            <span class="label">Delay:</span>
                            <span class="value delay">${vehicle.delay_minutes} min</span>
                        </div>
                    ` : ''}
                    ${vehicle.next_stop ? `
                        <div class="info-row">
                            <span class="label">Next Stop:</span>
                            <span class="value">${vehicle.next_stop}</span>
                        </div>
                    ` : ''}
                    <div class="info-row">
                        <span class="label">Last Update:</span>
                        <span class="value">${lastUpdate}</span>
                    </div>
                </div>
                <div class="popup-actions">
                    <button onclick="app.trackVehicle(${JSON.stringify(vehicle).replace(/"/g, '&quot;')})" class="track-btn">
                        📍 Track Vehicle
                    </button>
                </div>
            </div>
        `;
    }

    addVehicleTrail(vehicleId, fromPos, toPos, vehicleType) {
        if (!this.vehicleTrailsLayer) return;

        // Get or create trail for this vehicle
        let trail = this.vehicleTrails.get(vehicleId) || [];
        
        // Add new position to trail
        trail.push(toPos);
        
        // Keep only last 10 positions
        if (trail.length > 10) {
            trail = trail.slice(-10);
        }
        
        this.vehicleTrails.set(vehicleId, trail);

        // Create trail polyline
        if (trail.length > 1) {
            const trailColor = vehicleType === 'bus' ? '#2196F3' : '#9C27B0';
            const trailLine = L.polyline(trail, {
                color: trailColor,
                weight: 3,
                opacity: 0.6,
                dashArray: '5, 5'
            });
            
            this.vehicleTrailsLayer.addLayer(trailLine);
            
            // Remove old trails after 30 seconds
            setTimeout(() => {
                this.vehicleTrailsLayer.removeLayer(trailLine);
            }, 30000);
        }
    }

    animateVehicleMovement(marker, fromPos, toPos) {
        // Simple animation by updating marker position
        const duration = 1000; // 1 second
        const startTime = Date.now();
        
        const animate = () => {
            const elapsed = Date.now() - startTime;
            const progress = Math.min(elapsed / duration, 1);
            
            // Linear interpolation
            const lat = fromPos[0] + (toPos[0] - fromPos[0]) * progress;
            const lng = fromPos[1] + (toPos[1] - fromPos[1]) * progress;
            
            marker.setLatLng([lat, lng]);
            
            if (progress < 1) {
                requestAnimationFrame(animate);
            }
        };
        
        requestAnimationFrame(animate);
    }

    trackVehicle(vehicle) {
        // Center map on vehicle and show notification
        this.map.setView([vehicle.lat, vehicle.lng], 16);
        this.showNotification(`Now tracking ${vehicle.route_name || 'vehicle'}`, 'info');
        
        // Highlight the vehicle temporarily
        const vehicleMarker = this.findVehicleMarker(vehicle);
        if (vehicleMarker) {
            this.highlightVehicle(vehicleMarker);
        }
    }

    findVehicleMarker(vehicle) {
        // Find the marker for this vehicle
        let foundMarker = null;
        this.vehicleMarkers.eachLayer(marker => {
            const markerPos = marker.getLatLng();
            if (Math.abs(markerPos.lat - vehicle.lat) < 0.001 && 
                Math.abs(markerPos.lng - vehicle.lng) < 0.001) {
                foundMarker = marker;
            }
        });
        return foundMarker;
    }

    highlightVehicle(marker) {
        // Add temporary highlight effect
        const originalIcon = marker.getIcon();
        const highlightIcon = L.divIcon({
            className: 'vehicle-marker enhanced-vehicle-marker highlighted',
            html: originalIcon.options.html,
            iconSize: [50, 50],
            iconAnchor: [25, 25]
        });
        
        marker.setIcon(highlightIcon);
        
        // Remove highlight after 3 seconds
        setTimeout(() => {
            marker.setIcon(originalIcon);
        }, 3000);
    }

    updateVehicleCountDisplay(count) {
        // Update vehicle count in UI
        const countElement = document.getElementById('live-vehicle-count');
        if (countElement) {
            countElement.textContent = count;
        }
    }

    startLiveVehicleTracking() {
        // Start periodic updates every 10 seconds
        this.vehicleUpdateInterval = setInterval(() => {
            if (this.isLiveTrackingEnabled) {
                this.loadLiveVehicles();
            }
        }, 10000);
    }

    toggleLiveTracking() {
        this.isLiveTrackingEnabled = !this.isLiveTrackingEnabled;
        
        if (this.isLiveTrackingEnabled) {
            this.showNotification('Live vehicle tracking enabled', 'success');
            this.startLiveVehicleTracking();
        } else {
            this.showNotification('Live vehicle tracking disabled', 'info');
            if (this.vehicleUpdateInterval) {
                clearInterval(this.vehicleUpdateInterval);
            }
        }
        
        // Update UI toggle
        const toggleBtn = document.getElementById('live-tracking-toggle');
        if (toggleBtn) {
            toggleBtn.classList.toggle('active', this.isLiveTrackingEnabled);
        }
    }

    toggleVehicleType(type) {
        if (this.selectedVehicleTypes.has(type)) {
            this.selectedVehicleTypes.delete(type);
        } else {
            this.selectedVehicleTypes.add(type);
        }
        
        // Refresh vehicle display
        this.loadLiveVehicles();
        
        // Update UI
        const toggleBtn = document.getElementById(`${type}-toggle`);
        if (toggleBtn) {
            toggleBtn.classList.toggle('active', this.selectedVehicleTypes.has(type));
        }
    }

    clearVehicleTrails() {
        if (this.vehicleTrailsLayer) {
            this.vehicleTrailsLayer.clearLayers();
        }
        this.vehicleTrails.clear();
        this.showNotification('Vehicle trails cleared', 'info');
    }

    setupAddressSuggestions(inputType) {
        const input = document.getElementById(inputType);
        if (!input) return;
        
        let suggestionTimeout;
        let suggestionsContainer = document.getElementById(`${inputType}-suggestions`);
        
        if (!suggestionsContainer) {
            suggestionsContainer = document.createElement('div');
            suggestionsContainer.id = `${inputType}-suggestions`;
            suggestionsContainer.className = 'address-suggestions';
            input.parentNode.appendChild(suggestionsContainer);
        }
        
        input.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            
            clearTimeout(suggestionTimeout);
            
            if (query.length < 2) {
                suggestionsContainer.innerHTML = '';
                suggestionsContainer.style.display = 'none';
                return;
            }
            
            suggestionTimeout = setTimeout(async () => {
                try {
                    console.log(`Fetching suggestions for query: "${query}"`);
                    const response = await fetch(`/api/address-suggestions?q=${encodeURIComponent(query)}`);
                    
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    
                    const data = await response.json();
                    console.log('Received suggestions:', data);
                    
                    this.displayAddressSuggestions(data.suggestions, suggestionsContainer, input, inputType);
                } catch (error) {
                    console.error('Error fetching address suggestions:', error);
                    suggestionsContainer.innerHTML = '<div class="suggestion-item error">Error loading suggestions</div>';
                    suggestionsContainer.style.display = 'block';
                }
            }, 300);
        });
        
        // Hide suggestions when clicking outside
        document.addEventListener('click', (e) => {
            if (!input.contains(e.target) && !suggestionsContainer.contains(e.target)) {
                suggestionsContainer.style.display = 'none';
            }
        });
    }
    
    displayAddressSuggestions(suggestions, container, input, inputType) {
        container.innerHTML = '';
        
        if (suggestions.length === 0) {
            container.style.display = 'none';
            return;
        }
        
        suggestions.forEach(suggestion => {
            const suggestionElement = document.createElement('div');
            suggestionElement.className = 'suggestion-item';
            suggestionElement.innerHTML = `
                <div class="suggestion-address">${suggestion.address}</div>
                <div class="suggestion-type">${suggestion.type}</div>
            `;
            
            suggestionElement.addEventListener('click', () => {
                input.value = suggestion.address;
                container.style.display = 'none';
                
                const latlng = L.latLng(suggestion.lat, suggestion.lon);
                if (inputType === 'source') {
                    this.setSourceLocation(latlng, suggestion.address);
                } else {
                    this.setDestinationLocation(latlng, suggestion.address);
                }
            });
            
            container.appendChild(suggestionElement);
        });
        
        container.style.display = 'block';
    }
    
    async getCurrentLocation() {
        if (!navigator.geolocation) {
            this.showNotification('Geolocation is not supported by this browser', 'error');
            return;
        }
        
        this.showNotification('Getting your current location...', 'info');
        
        navigator.geolocation.getCurrentPosition(
            async (position) => {
                const lat = position.coords.latitude;
                const lon = position.coords.longitude;
                const latlng = L.latLng(lat, lon);
                
                try {
                    // Get location information from backend
                    const response = await fetch('/api/current-location', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ lat, lon })
                    });
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        // Remove existing current location marker
                        if (this.currentLocationMarker) {
                            this.map.removeLayer(this.currentLocationMarker);
                        }
                        
                        // Add current location marker
                        this.currentLocationMarker = L.marker(latlng, {
                            icon: L.divIcon({
                                className: 'current-location-marker',
                                html: '<div class="current-location-pulse"></div>',
                                iconSize: [20, 20],
                                iconAnchor: [10, 10]
                            })
                        }).addTo(this.map);
                        
                        // Center map on current location
                        this.map.setView(latlng, 15);
                        
                        // Set as source location
                        this.setSourceLocation(latlng, data.address);
                        document.getElementById('source').value = data.address;
                        
                        // Display nearest stops
                        if (data.nearest_stops && data.nearest_stops.length > 0) {
                            this.displayNearestStops(data.nearest_stops, 'current');
                        }
                        
                        this.showNotification('Current location set as source', 'success');
                    } else {
                        this.showNotification('Failed to get location information', 'error');
                    }
                } catch (error) {
                    console.error('Error getting current location info:', error);
                    this.showNotification('Error getting location information', 'error');
                }
            },
            (error) => {
                let message = 'Unable to get your location';
                switch (error.code) {
                    case error.PERMISSION_DENIED:
                        message = 'Location access denied by user';
                        break;
                    case error.POSITION_UNAVAILABLE:
                        message = 'Location information unavailable';
                        break;
                    case error.TIMEOUT:
                        message = 'Location request timed out';
                        break;
                }
                this.showNotification(message, 'error');
            },
            {
                enableHighAccuracy: true,
                timeout: 10000,
                maximumAge: 60000
            }
        );
    }
    
    initializeChatbot() {
        // Setup chatbot event listeners for existing HTML elements
        const chatbotInput = document.getElementById('chatbot-input');
        if (chatbotInput) {
            chatbotInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.sendChatbotMessage();
                }
            });
        }
        
        // Setup toggle button event listener as backup
        const chatbotToggle = document.getElementById('chatbot-toggle');
        if (chatbotToggle) {
            chatbotToggle.addEventListener('click', (e) => {
                e.preventDefault();
                this.toggleChatbot();
            });
        }
    }
    
    toggleChatbot() {
        const container = document.getElementById('chatbot-container');
        const toggle = document.getElementById('chatbot-toggle');
        
        this.chatbotVisible = !this.chatbotVisible;
        
        if (this.chatbotVisible) {
            container.style.display = 'flex';  // Use flex to maintain layout
            toggle.innerHTML = '<i class="fas fa-times"></i>';  // Change icon to close
        } else {
            container.style.display = 'none';
            toggle.innerHTML = '<i class="fas fa-comments"></i>';  // Change icon back to chat
        }
    }
    
    async sendChatbotMessage() {
        const input = document.getElementById('chatbot-input');
        const messagesContainer = document.getElementById('chatbot-messages');
        const query = input.value.trim();
        
        if (!query) return;
        
        // Add user message
        const userMessage = document.createElement('div');
        userMessage.className = 'user-message';
        userMessage.textContent = query;
        messagesContainer.appendChild(userMessage);
        
        // Clear input
        input.value = '';
        
        // Scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
        
        try {
            // Send to chatbot API
            const response = await fetch('/api/chatbot', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ query })
            });
            
            const data = await response.json();
            
            if (data.success) {
                // Add bot response
                const botMessage = document.createElement('div');
                botMessage.className = 'chatbot-message bot-message';
                
                let smartSuggestionsHtml = '';
                if (data.smart_suggestions && data.smart_suggestions.length > 0) {
                    smartSuggestionsHtml = `
                        <div class="smart-suggestions">
                            <strong>🧠 Smart Suggestions (Based on Historical Data):</strong>
                            <div class="suggestions-grid">
                                ${data.smart_suggestions.map(suggestion => `
                                    <div class="suggestion-item" data-confidence="${suggestion.confidence}">
                                        <span class="suggestion-icon">${suggestion.icon}</span>
                                        <span class="suggestion-text">${suggestion.text}</span>
                                        <span class="suggestion-confidence">${Math.round(suggestion.confidence * 100)}%</span>
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                    `;
                }
                
                botMessage.innerHTML = `
                    <i class="fas fa-robot"></i>
                    <span>
                        <div class="bot-response">${data.response}</div>
                        ${smartSuggestionsHtml}
                        ${data.tips && data.tips.length > 0 ? `
                            <div class="bot-tips">
                                <strong>💡 Tips:</strong>
                                <ul>
                                    ${data.tips.map(tip => `<li>${tip}</li>`).join('')}
                                </ul>
                            </div>
                        ` : ''}
                        ${data.realtime_data && data.realtime_data.historical_insights_available ? `
                            <div class="data-sources">
                                <small>📊 Powered by: ${data.realtime_data.data_sources.join(', ')}</small>
                            </div>
                        ` : ''}
                    </span>
                `;
                messagesContainer.appendChild(botMessage);
            } else {
                const errorMessage = document.createElement('div');
                errorMessage.className = 'chatbot-message bot-message error';
                errorMessage.innerHTML = `
                    <i class="fas fa-robot"></i>
                    <span>Sorry, I encountered an error. Please try again.</span>
                `;
                messagesContainer.appendChild(errorMessage);
            }
            
            // Scroll to bottom after adding bot response
            messagesContainer.scrollTop = messagesContainer.scrollHeight;
        } catch (error) {
            console.error('Chatbot error:', error);
            const errorMessage = document.createElement('div');
            errorMessage.className = 'chatbot-message bot-message error';
            errorMessage.innerHTML = `
                <i class="fas fa-robot"></i>
                <span>Sorry, I'm having trouble connecting. Please try again.</span>
            `;
            messagesContainer.appendChild(errorMessage);
        }
        
        // Always scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    }
    
    async getLLMRecommendations() {
        const sourceInput = document.getElementById('source').value;
        const destinationInput = document.getElementById('destination').value;
        
        if ((!this.sourceMarker || !this.destinationMarker) && (!sourceInput || !destinationInput)) {
            this.showNotification('Please set both source and destination first', 'warning');
            return;
        }
        
        try {
            const sourceInput = document.getElementById('source').value;
            const destinationInput = document.getElementById('destination').value;
            const currentTime = new Date().toTimeString().slice(0, 5);
            
            const response = await fetch('/api/llm-recommendations', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    source: sourceInput,
                    destination: destinationInput,
                    time_of_day: currentTime,
                    preferences: {
                        priority: 'balanced' // Can be 'cost', 'time', 'comfort'
                    }
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                this.displayLLMRecommendations(data.recommendation, data.peak_hour_info);
            }
        } catch (error) {
            console.error('Error getting LLM recommendations:', error);
            this.showNotification('Failed to get intelligent recommendations', 'error');
        }
    }
    
    displayLLMRecommendations(recommendation, peakHourInfo) {
        const recommendationsContainer = document.getElementById('llm-recommendations');
        if (!recommendationsContainer) return;
        
        recommendationsContainer.innerHTML = `
            <div class="llm-recommendation">
                <h4>🤖 AI Recommendation</h4>
                <div class="recommended-mode">
                    <strong>Best Option:</strong> ${recommendation.recommended_mode}
                </div>
                <div class="reasoning">
                    <strong>Why:</strong> ${recommendation.reasoning}
                </div>
                <div class="estimates">
                    <span class="time-estimate">⏱️ ${recommendation.time_estimate}</span>
                    <span class="cost-estimate">💰 ${recommendation.cost_estimate}</span>
                    <span class="comfort-rating">⭐ ${recommendation.comfort_rating}/5</span>
                </div>
                <div class="alternatives">
                    <strong>Alternatives:</strong> ${recommendation.alternatives.join(', ')}
                </div>
                <div class="tips">
                    <strong>Tips:</strong>
                    <ul>
                        ${recommendation.tips.map(tip => `<li>${tip}</li>`).join('')}
                    </ul>
                </div>
                ${peakHourInfo.is_peak_hour ? `
                    <div class="peak-hour-warning">
                        ⚠️ Peak hour traffic - expect delays
                    </div>
                ` : ''}
            </div>
        `;
        
        recommendationsContainer.style.display = 'block';
    }

    async calculatePricing() {
        // Get real-time pricing for all transport modes if coordinates are available
        console.log('calculatePricing called:', {
            sourceLocation: this.sourceLocation,
            destinationLocation: this.destinationLocation
        });
        
        if (!this.sourceLocation || !this.destinationLocation) {
            console.log('Missing coordinates, setting currentPricing to null');
            this.currentPricing = null;
            return;
        }

        try {
            const requestData = {
                source_lat: this.sourceLocation.lat,
                source_lng: this.sourceLocation.lng,
                dest_lat: this.destinationLocation.lat,
                dest_lng: this.destinationLocation.lng,
                source_name: this.sourceLocation.name || 'Source',
                dest_name: this.destinationLocation.name || 'Destination'
            };

            console.log('calculatePricing - Request data:', requestData);

            const response = await fetch('/api/transport/all-options', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestData)
            });

            console.log('calculatePricing - Response status:', response.status);

            if (response.ok) {
                const responseData = await response.json();
                console.log('calculatePricing - API Response:', responseData);
                
                // The API response structure is: { data: { fare_options: {...}, pathway_realtime: {...}, ... }, success: true }
                const apiData = responseData.data || {};
                const fareData = {
                    fare_options: apiData.fare_options || {},
                    pathway_realtime: apiData.pathway_realtime || {},
                    cheapest_option: apiData.cheapest_option || null,
                    alternatives_available: apiData.alternatives_available || false
                };
                
                // If fare_options is null or empty, use fallback pricing
                if (!fareData.fare_options || Object.keys(fareData.fare_options).length === 0) {
                    console.log('No fare options available from API, using fallback pricing');
                    this.fallbackPricing();
                    return;
                }
                
                console.log('calculatePricing - fare_options keys:', Object.keys(fareData.fare_options));
                console.log('calculatePricing - pathway_realtime available:', !!fareData.pathway_realtime);
                this.currentPricing = fareData;
                console.log('calculatePricing - Stored currentPricing:', this.currentPricing);
                this.displayTransportOptions(fareData);
            } else {
                console.error('Failed to fetch fare data:', response.statusText);
                this.fallbackPricing();
            }
        } catch (error) {
            console.error('Error calculating pricing:', error);
            this.fallbackPricing();
        }
    }

    fallbackPricing() {
        // Fallback to basic pricing calculation
        const mode = this.selectedTransitType;
        let baseFare = 0;
        let perKmRate = 0;
        
        switch (mode) {
            case 'bmtc-ordinary':
                baseFare = 5;
                perKmRate = 1.5;
                break;
            case 'bmtc-ac':
                baseFare = 10;
                perKmRate = 2.0;
                break;
            case 'bmrcl-metro':
                baseFare = 10;
                perKmRate = 2.5;
                break;
            case 'taxi':
                baseFare = 50;
                perKmRate = 12;
                break;
            default:
                baseFare = 5;
                perKmRate = 1.5;
        }
        
        if (this.sourceCoords && this.destinationCoords) {
            const distance = this.calculateDistance(
                this.sourceCoords.lat, this.sourceCoords.lng,
                this.destinationCoords.lat, this.destinationCoords.lng
            );
            const estimatedFare = baseFare + (distance * perKmRate);
            this.currentPricing = {
                fare_options: {
                    [mode.replace('-', '_')]: {
                        fare: Math.round(estimatedFare),
                        distance_km: distance,
                        duration_minutes: Math.round(distance / 0.5), // Rough estimate
                        transport_type: mode
                    }
                }
            };
        }
    }

    displayTransportOptions(fareData) {
        console.log('displayTransportOptions called with:', fareData);
        const transportContainer = document.querySelector('.transit-types');
        if (!transportContainer || !fareData || !fareData.fare_options) {
            console.log('displayTransportOptions early return:', {
                hasContainer: !!transportContainer,
                hasFareData: !!fareData,
                hasFareOptions: !!(fareData && fareData.fare_options)
            });
            return;
        }

        // Clear existing options
        transportContainer.innerHTML = '';

        // Create transport option cards for each available mode
        console.log('displayTransportOptions - Creating cards for modes:', Object.keys(fareData.fare_options));
        Object.entries(fareData.fare_options).forEach(([modeKey, modeData]) => {
            console.log('displayTransportOptions - Creating card for:', modeKey, modeData);
            const transportCard = this.createTransportCard(modeKey, modeData);
            transportContainer.appendChild(transportCard);
        });

        // Select the first option by default
        const firstCard = transportContainer.querySelector('.transit-type');
        if (firstCard) {
            firstCard.classList.add('selected');
            this.selectedTransitType = firstCard.dataset.type;
            console.log('displayTransportOptions - Selected first card:', this.selectedTransitType);
            // Show details for the default selected transport option
            this.showTransportDetails();
        }
    }

    createTransportCard(modeKey, modeData) {
        const card = document.createElement('div');
        card.className = 'transit-type';
        card.dataset.type = modeKey;
        
        // Get transport name from route_description or mode
        const transportName = modeData.route_description || modeData.mode || modeKey;
        
        // Get icon and color based on transport type
        const { icon, color } = this.getTransportIcon(transportName);
        
        // Format fare display
        let fareDisplay = '₹--';
        if (typeof modeData.fare === 'number') {
            fareDisplay = `₹${modeData.fare.toFixed(2)}`;
        }

        card.innerHTML = `
            <div class="transport-icon" style="background-color: ${color}">
                <i class="fas ${icon}"></i>
            </div>
            <div class="transport-info">
                <div class="transport-name">${transportName}</div>
                <div class="transport-fare">${fareDisplay}</div>
                <div class="transport-time">${modeData.travel_time_minutes || 'N/A'} min</div>
                ${modeData.surge_multiplier && modeData.surge_multiplier > 1 ? 
                    `<div class="surge-indicator">SURGE ${modeData.surge_multiplier}x</div>` : ''}
            </div>
        `;

        // Add click event listener
        card.addEventListener('click', () => {
            document.querySelectorAll('.transit-type').forEach(el => el.classList.remove('selected'));
            card.classList.add('selected');
            this.selectedTransitType = modeKey;
            this.showTransportDetails();
        });

        return card;
    }

    getTransportIcon(transportType) {
        const iconMap = {
            'BMTC Ordinary Bus': { icon: 'fa-bus', color: '#2196F3' },
            'BMTC Deluxe Bus': { icon: 'fa-bus', color: '#4CAF50' },
            'BMTC AC Bus': { icon: 'fa-bus', color: '#FF9800' },
            'BMTC Vajra Bus': { icon: 'fa-bus', color: '#9C27B0' },
            'BMRCL Metro (Token)': { icon: 'fa-train', color: '#E91E63' },
            'BMRCL Metro (Smart Card)': { icon: 'fa-train', color: '#E91E63' },
            'Taxi/Cab': { icon: 'fa-car', color: '#FFC107' },
            'Auto Rickshaw': { icon: 'fa-taxi', color: '#795548' },
            'Walking': { icon: 'fa-walking', color: '#607D8B' },
            'Cycling': { icon: 'fa-bicycle', color: '#4CAF50' }
        };

        return iconMap[transportType] || { icon: 'fa-bus', color: '#2196F3' };
    }

    async showTransportDetails() {
        const transportDetails = document.getElementById('transport-details');
        const transportDetailsContent = document.getElementById('transport-details-content');
        
        if (!transportDetails || !transportDetailsContent) return;
        
        const mode = this.selectedTransitType;
        
        // Since we're using dynamic transport cards, the selectedTransitType should already be the correct API key
        // But we'll keep a fallback mapping for any legacy static transport types
        const modeMapping = {
            'bmtc-ordinary': 'bmtc_ordinary',
            'bmtc-ac': 'bmtc_ac',
            'bmtc-deluxe': 'bmtc_deluxe',
            'bmrcl-metro': 'bmrcl_token', // Default to token for metro
            'taxi': 'taxi',
            'auto': 'auto',
            'walking': 'walking',
            'cycling': 'cycling'
        };
        
        // The mode should already be the correct API key from dynamic transport cards
        let fareOptionKey = mode;
        
        // Fallback: if the mode is not found in fare_options, try mapping (for legacy static types)
        if (this.currentPricing && this.currentPricing.fare_options && !this.currentPricing.fare_options[mode]) {
            if (modeMapping[mode]) {
                fareOptionKey = modeMapping[mode];
            }
        }
        
        console.log('Transport Details Debug:', {
            mode: mode,
            fareOptionKey: fareOptionKey,
            hasPricing: !!this.currentPricing,
            fareOptions: this.currentPricing?.fare_options ? Object.keys(this.currentPricing.fare_options) : null,
            currentPricing: this.currentPricing,
            selectedTransitType: this.selectedTransitType,
            modeMapping: modeMapping,
            availableFareOptions: this.currentPricing?.fare_options
        });
        
        // Get the selected transport mode data
        let selectedModeData = null;
        if (this.currentPricing && this.currentPricing.fare_options && this.currentPricing.fare_options[fareOptionKey]) {
            selectedModeData = this.currentPricing.fare_options[fareOptionKey];
        }
        
        if (!selectedModeData) {
            // Provide more helpful information when data is not available
            transportDetailsContent.innerHTML = `
                <div class="no-data-message">
                    <h4>Transport Details</h4>
                    <p>Detailed fare information is currently unavailable for the selected transport mode.</p>
                    <div class="fallback-info">
                        <h5>General Information:</h5>
                        <p><strong>Selected Mode:</strong> ${mode.replace(/[_-]/g, ' ').toUpperCase()}</p>
                        <p><strong>Status:</strong> Service information is being updated</p>
                        <p><strong>Note:</strong> Please try again in a few moments or select a different transport option.</p>
                    </div>
                    <div class="help-text">
                        <small>💡 Tip: Make sure both source and destination are selected for accurate fare calculation.</small>
                    </div>
                </div>
            `;
            transportDetails.style.display = 'block';
            return;
        }
        
        // Format fare display
        let fareDisplay = '₹--';
        let fareBreakdown = '';
        
        if (typeof selectedModeData.fare === 'number') {
            fareDisplay = `₹${selectedModeData.fare.toFixed(2)}`;
            
            // Create fare breakdown with available data
            fareBreakdown = `
                <div class="fare-breakdown">
                    <h5>Fare Details</h5>
                    <div class="breakdown-item">
                        <span>Total Fare:</span>
                        <span>₹${selectedModeData.fare.toFixed(2)}</span>
                    </div>
                    <div class="breakdown-item">
                        <span>Distance:</span>
                        <span>${selectedModeData.distance_km ? selectedModeData.distance_km.toFixed(1) + ' km' : 'N/A'}</span>
                    </div>
                    ${selectedModeData.booking_fee && selectedModeData.booking_fee > 0 ? 
                        `<div class="breakdown-item">
                            <span>Booking Fee:</span>
                            <span>₹${selectedModeData.booking_fee.toFixed(2)}</span>
                        </div>` : ''}
                    ${selectedModeData.surge_multiplier && selectedModeData.surge_multiplier > 1 ? 
                        `<div class="breakdown-item surge">
                            <span>Surge (${selectedModeData.surge_multiplier}x):</span>
                            <span>Applied</span>
                        </div>` : ''}
                    ${selectedModeData.confidence_score ? 
                        `<div class="breakdown-item">
                            <span>Confidence:</span>
                            <span>${(selectedModeData.confidence_score * 100).toFixed(0)}%</span>
                        </div>` : ''}
                </div>
            `;
        }

        let content = `
            <div class="transport-info-grid">
                <div class="transport-info-card">
                    <h4>Selected Mode</h4>
                    <p class="value">${selectedModeData.route_description || selectedModeData.mode || 'N/A'}</p>
                </div>
                <div class="transport-info-card">
                    <h4>Total Cost</h4>
                    <p class="value">${fareDisplay}</p>
                </div>
                <div class="transport-info-card">
                    <h4>Travel Time</h4>
                    <p class="value">${selectedModeData.travel_time_minutes || 'N/A'} min</p>
                </div>
                <div class="transport-info-card">
                    <h4>Distance</h4>
                    <p class="value">${(selectedModeData.distance_km || 0).toFixed(1)} km</p>
                </div>
            </div>
            
            <div class="transport-details-section">
                <h4>Transport Details</h4>
                <div class="detail-grid">
                    <div class="detail-item">
                        <span class="label">Provider:</span>
                        <span class="value">${selectedModeData.provider || 'N/A'}</span>
                    </div>
                    <div class="detail-item">
                        <span class="label">Mode:</span>
                        <span class="value">${selectedModeData.mode || 'N/A'}</span>
                    </div>
                    ${selectedModeData.next_availability_minutes ? 
                        `<div class="detail-item">
                            <span class="label">Next Available:</span>
                            <span class="value">${selectedModeData.next_availability_minutes} min</span>
                        </div>` : ''}
                    ${selectedModeData.surge_multiplier && selectedModeData.surge_multiplier > 1 ? 
                        `<div class="detail-item surge">
                            <span class="label">Surge Multiplier:</span>
                            <span class="value">${selectedModeData.surge_multiplier}x</span>
                        </div>` : ''}
                    ${selectedModeData.fallback_used ? 
                        `<div class="detail-item">
                            <span class="label">Data Source:</span>
                            <span class="value">Estimated</span>
                        </div>` : 
                        `<div class="detail-item">
                            <span class="label">Data Source:</span>
                            <span class="value">Live</span>
                        </div>`}
                </div>
            </div>
            
            ${fareBreakdown}
            
            ${this.renderPathwayRealTimeData()}
        `;
        

        
        transportDetailsContent.innerHTML = content;
        transportDetails.style.display = 'block';
    }

    renderPathwayRealTimeData() {
        if (!this.currentPricing || !this.currentPricing.pathway_realtime) {
            return '';
        }

        const pathwayData = this.currentPricing.pathway_realtime;
        let realTimeContent = '<div class="pathway-realtime-section"><h4>🚌 Live Transport Updates</h4>';

        // Real-time buses
        if (pathwayData.real_time_buses && pathwayData.real_time_buses.length > 0) {
            realTimeContent += `
                <div class="realtime-subsection">
                    <h5><i class="fas fa-bus"></i> Live Buses Nearby</h5>
                    <div class="realtime-grid">
            `;
            
            pathwayData.real_time_buses.slice(0, 3).forEach(bus => {
                realTimeContent += `
                    <div class="realtime-item bus">
                        <div class="route-info">
                            <span class="route-number">${bus.route_number || 'N/A'}</span>
                            <span class="destination">${bus.destination || 'Unknown'}</span>
                        </div>
                        <div class="timing-info">
                            <span class="eta">${bus.eta_minutes || 'N/A'} min</span>
                            <span class="status ${bus.status?.toLowerCase() || 'unknown'}">${bus.status || 'Unknown'}</span>
                        </div>
                    </div>
                `;
            });
            
            realTimeContent += '</div></div>';
        }

        // Real-time metros
        if (pathwayData.real_time_metros && pathwayData.real_time_metros.length > 0) {
            realTimeContent += `
                <div class="realtime-subsection">
                    <h5><i class="fas fa-train"></i> Live Metro Updates</h5>
                    <div class="realtime-grid">
            `;
            
            pathwayData.real_time_metros.slice(0, 3).forEach(metro => {
                realTimeContent += `
                    <div class="realtime-item metro">
                        <div class="route-info">
                            <span class="line-name">${metro.line_name || 'Metro Line'}</span>
                            <span class="direction">${metro.direction || 'Unknown Direction'}</span>
                        </div>
                        <div class="timing-info">
                            <span class="eta">${metro.eta_minutes || 'N/A'} min</span>
                            <span class="status ${metro.status?.toLowerCase() || 'unknown'}">${metro.status || 'Unknown'}</span>
                        </div>
                    </div>
                `;
            });
            
            realTimeContent += '</div></div>';
        }

        // Traffic conditions
        if (pathwayData.traffic_conditions && pathwayData.traffic_conditions.length > 0) {
            realTimeContent += `
                <div class="realtime-subsection">
                    <h5><i class="fas fa-road"></i> Traffic Conditions</h5>
                    <div class="traffic-grid">
            `;
            
            pathwayData.traffic_conditions.slice(0, 2).forEach(traffic => {
                const congestionColor = this.getTrafficColor(traffic.congestion_level);
                realTimeContent += `
                    <div class="traffic-item">
                        <div class="road-info">
                            <span class="road-name">${traffic.road_name || 'Road'}</span>
                            <span class="congestion-level" style="color: ${congestionColor}">
                                ${traffic.congestion_level || 'Unknown'}
                            </span>
                        </div>
                        <div class="speed-info">
                            <span class="speed">${traffic.average_speed || 'N/A'} km/h</span>
                        </div>
                    </div>
                `;
            });
            
            realTimeContent += '</div></div>';
        }

        // Service alerts
        if (pathwayData.service_alerts && pathwayData.service_alerts.length > 0) {
            realTimeContent += `
                <div class="realtime-subsection">
                    <h5><i class="fas fa-exclamation-triangle"></i> Service Alerts</h5>
                    <div class="alerts-grid">
            `;
            
            pathwayData.service_alerts.slice(0, 2).forEach(alert => {
                const alertType = alert.alert_type?.toLowerCase() || 'info';
                realTimeContent += `
                    <div class="alert-item ${alertType}">
                        <div class="alert-header">
                            <span class="alert-type">${alert.alert_type || 'Alert'}</span>
                            <span class="alert-severity">${alert.severity || 'Medium'}</span>
                        </div>
                        <div class="alert-message">${alert.message || 'Service alert active'}</div>
                        ${alert.affected_routes ? `<div class="affected-routes">Routes: ${alert.affected_routes.join(', ')}</div>` : ''}
                    </div>
                `;
            });
            
            realTimeContent += '</div></div>';
        }

        realTimeContent += '</div>';
        
        return realTimeContent;
    }
}

// Utility functions
function getCurrentLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const lat = position.coords.latitude;
                const lng = position.coords.longitude;
                app.setSourceLocation(L.latLng(lat, lng), 'Current Location');
                app.map.setView([lat, lng], 15);
                app.showNotification('Current location set as source', 'success');
            },
            (error) => {
                console.error('Geolocation error:', error);
                app.showNotification('Unable to get current location', 'error');
            },
            {
                enableHighAccuracy: true,
                timeout: 10000,
                maximumAge: 300000
            }
        );
    } else {
        app.showNotification('Geolocation is not supported by this browser', 'error');
    }
}

function clearLocations() {
    if (app.sourceMarker) {
        app.map.removeLayer(app.sourceMarker);
        app.sourceMarker = null;
    }
    if (app.destinationMarker) {
        app.map.removeLayer(app.destinationMarker);
        app.destinationMarker = null;
    }
    
    // Safely clear layers
    if (app.routeLayer) {
        app.routeLayer.clearLayers();
    }
    if (app.nearestStopsLayer) {
        app.nearestStopsLayer.clearLayers();
    }
    if (app.trafficLayer) {
        app.trafficLayer.clearLayers();
    }
    if (app.vehicleMarkers) {
        app.vehicleMarkers.clearLayers();
    }
    
    // Clear input fields
    document.getElementById('source').value = '';
    document.getElementById('destination').value = '';
    
    // Clear results and transport details
    document.getElementById('results').innerHTML = '';
    const transportDetails = document.getElementById('transport-details');
    if (transportDetails) {
        transportDetails.innerHTML = '';
    }
    
    // Reset source/destination coordinates
    app.sourceCoords = null;
    app.destinationCoords = null;
    
    app.isSelectingSource = true;
    app.currentRoute = null;
    
    app.showNotification('All locations and routes cleared', 'info');
}

// Initialize the application
let app;

// Global functions for HTML onclick handlers
function sendChatbotMessage() {
    if (app) {
        app.sendChatbotMessage();
    }
}

function toggleChatbot() {
    if (app) {
        app.toggleChatbot();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    app = new BangaloreTransitApp();
});