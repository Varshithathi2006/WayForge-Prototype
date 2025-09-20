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
        this.vehicleMarkers = [];
        
        // Bangalore coordinates
        this.bangaloreCenter = [12.9716, 77.5946];
        
        this.initializeMap();
        this.setupEventListeners();
        this.connectWebSocket();
        this.loadLiveVehicles();
    }
    
    initializeMap() {
        // Initialize map centered on Bangalore
        this.map = L.map('map').setView(this.bangaloreCenter, 12);
        
        // Add OpenStreetMap tiles
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors',
            maxZoom: 18
        }).addTo(this.map);
        
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
            });
        });
        
        // Set default selection
        document.querySelector('.transit-type').classList.add('selected');
        
        // Location input fields
        document.getElementById('source').addEventListener('input', (e) => {
            this.searchLocation(e.target.value, 'source');
        });
        
        document.getElementById('destination').addEventListener('input', (e) => {
            this.searchLocation(e.target.value, 'destination');
        });
    }
    
    selectLocation(latlng) {
        if (this.isSelectingSource) {
            this.setSourceLocation(latlng);
        } else {
            this.setDestinationLocation(latlng);
        }
    }
    
    setSourceLocation(latlng) {
        if (this.sourceMarker) {
            this.map.removeLayer(this.sourceMarker);
        }
        
        this.sourceMarker = L.marker(latlng, {
            icon: L.divIcon({
                html: '🟢',
                iconSize: [30, 30],
                className: 'custom-marker'
            })
        }).addTo(this.map);
        
        this.reverseGeocode(latlng, 'source');
        this.isSelectingSource = false;
        this.calculatePricing();
    }
    
    setDestinationLocation(latlng) {
        if (this.destinationMarker) {
            this.map.removeLayer(this.destinationMarker);
        }
        
        this.destinationMarker = L.marker(latlng, {
            icon: L.divIcon({
                html: '🔴',
                iconSize: [30, 30],
                className: 'custom-marker'
            })
        }).addTo(this.map);
        
        this.reverseGeocode(latlng, 'destination');
        this.isSelectingSource = true;
        this.calculatePricing();
    }
    
    async reverseGeocode(latlng, type) {
        try {
            const response = await fetch(
                `https://nominatim.openstreetmap.org/reverse?format=json&lat=${latlng.lat}&lon=${latlng.lng}&zoom=18&addressdetails=1`
            );
            const data = await response.json();
            
            const address = data.display_name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
            document.getElementById(type).value = address;
        } catch (error) {
            console.error('Reverse geocoding failed:', error);
            document.getElementById(type).value = `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`;
        }
    }
    
    async searchLocation(query, type) {
        if (query.length < 3) return;
        
        try {
            const response = await fetch(
                `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query + ', Bangalore, India')}&limit=5`
            );
            const results = await response.json();
            
            if (results.length > 0) {
                const result = results[0];
                const latlng = L.latLng(result.lat, result.lon);
                
                if (type === 'source') {
                    this.setSourceLocation(latlng);
                } else {
                    this.setDestinationLocation(latlng);
                }
            }
        } catch (error) {
            console.error('Location search failed:', error);
        }
    }
    
    calculateDistance(lat1, lon1, lat2, lon2) {
        // Check for valid coordinates
        if (!lat1 || !lon1 || !lat2 || !lon2 || 
            isNaN(lat1) || isNaN(lon1) || isNaN(lat2) || isNaN(lon2)) {
            return 0;
        }
        
        const R = 6371; // Earth's radius in kilometers
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLon = (lon2 - lon1) * Math.PI / 180;
        const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
                  Math.sin(dLon/2) * Math.sin(dLon/2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        const distance = R * c;
        
        // Return 0 if distance is NaN or invalid
        return isNaN(distance) ? 0 : distance;
    }
    
    async calculatePricing() {
        if (!this.sourceMarker || !this.destinationMarker) {
            document.getElementById('results').style.display = 'none';
            return;
        }
        
        const sourceLatlng = this.sourceMarker.getLatLng();
        const destLatlng = this.destinationMarker.getLatLng();
        
        try {
            const response = await fetch('/api/calculate_fare', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    source_lat: sourceLatlng.lat,
                    source_lng: sourceLatlng.lng,
                    dest_lat: destLatlng.lat,
                    dest_lng: destLatlng.lng,
                    source_name: document.getElementById('source').value || 'Source',
                    dest_name: document.getElementById('destination').value || 'Destination',
                    transport_mode: this.selectedTransitType.split('-')[0] // Extract 'bus' or 'metro'
                })
            });

            if (response.ok) {
                const data = await response.json();
                this.currentRoute = data.route_data;
                this.displayFareResults(data.fare_results);
                this.drawRoute(sourceLatlng, destLatlng);
            } else {
                throw new Error('Failed to calculate fare');
            }
        } catch (error) {
            console.error('Error calculating fare:', error);
            // Fallback to original calculation
            const distance = this.calculateDistance(
                sourceLatlng.lat, sourceLatlng.lng,
                destLatlng.lat, destLatlng.lng
            );
            
            // Draw route line
            this.drawRoute(sourceLatlng, destLatlng);
            
            // Calculate pricing based on selected transit type
            const pricing = await this.getPricing(distance, this.selectedTransitType);
            this.displayResults(distance, pricing);
        }
    }
    
    drawRoute(source, destination) {
        if (this.routeLine) {
            this.map.removeLayer(this.routeLine);
        }
        
        this.routeLine = L.polyline([source, destination], {
            color: '#667eea',
            weight: 4,
            opacity: 0.7
        }).addTo(this.map);
        
        // Fit map to show both markers
        const group = new L.featureGroup([this.sourceMarker, this.destinationMarker]);
        this.map.fitBounds(group.getBounds().pad(0.1));
    }
    
    async getPricing(distance, transitType) {
        try {
            const response = await fetch('/api/calculate_fare', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    distance: distance,
                    transit_type: transitType
                })
            });
            
            if (!response.ok) {
                throw new Error('API request failed');
            }
            
            return await response.json();
        } catch (error) {
            console.error('Pricing calculation failed:', error);
            // Fallback to client-side calculation
            return this.calculateFareClientSide(distance, transitType);
        }
    }
    
    calculateFareClientSide(distance, transitType) {
        // Check for valid distance
        if (!distance || isNaN(distance) || distance <= 0) {
            distance = 0;
        }
        
        const fareStructures = {
            'bmtc-ordinary': { base: 5, perKm: 1, name: 'BMTC Ordinary Bus' },
            'bmtc-ac': { base: 15, perKm: 2, name: 'BMTC AC Bus' },
            'bmtc-vajra': { base: 25, perKm: 3, name: 'BMTC Vajra' },
            'bmrcl-metro': { 
                name: 'BMRCL Metro',
                slabs: [
                    { max: 2, fare: 10 },
                    { max: 5, fare: 15 },
                    { max: 8, fare: 20 },
                    { max: 12, fare: 25 },
                    { max: 20, fare: 30 },
                    { max: 30, fare: 40 },
                    { max: 40, fare: 50 },
                    { max: Infinity, fare: 60 }
                ]
            },
            'taxi': { base: 50, perKm: 15, name: 'Taxi Service' }
        };
        
        const structure = fareStructures[transitType];
        let totalFare = 0;
        
        if (transitType === 'bmrcl-metro') {
            const slab = structure.slabs.find(s => distance <= s.max);
            totalFare = slab ? slab.fare : 10; // Default to minimum fare if no slab found
        } else {
            totalFare = structure.base + (distance * structure.perKm);
        }
        
        // Ensure totalFare is not NaN
        if (isNaN(totalFare)) {
            totalFare = 0;
        }
        
        return {
            distance_km: distance,
            transit_type: transitType,
            transit_name: structure.name,
            total_fare: Math.round(totalFare),
            currency: 'INR',
            timestamp: Date.now()
        };
    }
    
    displayResults(distance, pricing) {
        const resultsDiv = document.getElementById('results');
        const contentDiv = document.getElementById('pricing-content');
        
        const html = `
            <div class="price-item">
                <span class="price-label">Distance</span>
                <span class="price-value">${distance.toFixed(2)} km</span>
            </div>
            <div class="price-item">
                <span class="price-label">Transit Type</span>
                <span class="price-value">${pricing.transit_name || pricing.transit_type}</span>
            </div>
            <div class="price-item">
                <span class="price-label">Total Fare</span>
                <span class="price-value">₹${pricing.total_fare}</span>
            </div>
            ${pricing.base_fare ? `
            <div class="price-item">
                <span class="price-label">Base Fare</span>
                <span class="price-value">₹${pricing.base_fare}</span>
            </div>
            <div class="price-item">
                <span class="price-label">Distance Fare</span>
                <span class="price-value">₹${pricing.distance_fare || (pricing.total_fare - pricing.base_fare)}</span>
            </div>
            ` : ''}
            ${pricing.smart_card_discount ? `
            <div class="price-item">
                <span class="price-label">Smart Card Discount</span>
                <span class="price-value">${pricing.smart_card_discount}</span>
            </div>
            ` : ''}
            <div class="route-info">
                <h4>🗺️ Route Information</h4>
                <p><strong>Estimated Travel Time:</strong> ${this.estimateTravelTime(distance, pricing.transit_type)}</p>
                <p><strong>Best Route:</strong> Direct route via major roads</p>
                ${pricing.transit_type === 'bmrcl-metro' ? 
                    '<p><strong>Note:</strong> Metro fare includes 5% smart card discount</p>' : 
                    '<p><strong>Note:</strong> Bus fare may vary based on actual route taken</p>'
                }
            </div>
        `;
        
        contentDiv.innerHTML = html;
        resultsDiv.style.display = 'block';
    }
    
    estimateTravelTime(distance, transitType) {
        const speeds = {
            'bmtc-ordinary': 15, // km/h in city traffic
            'bmtc-ac': 18,
            'bmtc-vajra': 20,
            'bmrcl-metro': 35,
            'taxi': 25 // km/h in city traffic
        };
        
        const speed = speeds[transitType] || 15;
        const timeHours = distance / speed;
        const timeMinutes = Math.round(timeHours * 60);
        
        return `${timeMinutes} minutes`;
    }
    
    async loadLiveVehicles() {
        try {
            // Load live vehicle positions
            const [bmtcResponse, bmrclResponse] = await Promise.all([
                fetch('/api/live/bmtc'),
                fetch('/api/live/bmrcl')
            ]);
            
            if (bmtcResponse.ok) {
                const bmtcData = await bmtcResponse.json();
                this.displayLiveVehicles(bmtcData, 'bus');
            }
            
            if (bmrclResponse.ok) {
                const bmrclData = await bmrclResponse.json();
                this.displayLiveVehicles(bmrclData, 'metro');
            }
        } catch (error) {
            console.log('Live vehicle data not available:', error);
        }
    }
    
    displayLiveVehicles(data, type) {
        if (!data.entity) return;
        
        data.entity.forEach(vehicle => {
            if (vehicle.vehicle && vehicle.vehicle.position) {
                const pos = vehicle.vehicle.position;
                const icon = type === 'bus' ? '🚌' : '🚇';
                
                L.marker([pos.latitude, pos.longitude], {
                    icon: L.divIcon({
                        html: icon,
                        iconSize: [20, 20],
                        className: 'live-vehicle'
                    })
                }).addTo(this.map)
                .bindPopup(`${icon} ${vehicle.vehicle.vehicle_id}<br>Speed: ${pos.speed?.toFixed(1) || 'N/A'} km/h`);
            }
        });
    }
    
    connectWebSocket() {
        try {
            this.websocket = new WebSocket('ws://localhost:8081');
            
            this.websocket.onopen = () => {
                console.log('WebSocket connected for real-time updates');
                this.showNotification('Connected to real-time data stream', 'success');
            };
            
            this.websocket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    this.handleRealtimeUpdate(data);
                } catch (error) {
                    console.error('Error parsing WebSocket message:', error);
                }
            };
            
            this.websocket.onclose = () => {
                console.log('WebSocket disconnected');
                this.showNotification('Real-time data connection lost', 'warning');
                
                // Attempt to reconnect after 5 seconds
                setTimeout(() => {
                    this.connectWebSocket();
                }, 5000);
            };
            
            this.websocket.onerror = (error) => {
                console.error('WebSocket error:', error);
                this.showNotification('Real-time data connection error', 'error');
            };
            
        } catch (error) {
            console.error('Error connecting to WebSocket:', error);
            // Continue with polling fallback
        }
    }
    
    handleRealtimeUpdate(data) {
        if (!data || !data.type) {
            console.warn('Invalid WebSocket data received:', data);
            return;
        }
        
        switch (data.type) {
            case 'vehicle_update':
                if (data.vehicles) {
                    this.updateVehiclePositions(data.vehicles);
                }
                break;
            case 'fare_update':
                if (data.fares) {
                    this.updateFareInformation(data.fares);
                }
                break;
            case 'route_response':
                if (data.route) {
                    this.handleRouteResponse(data.route);
                }
                break;
            case 'nearby_vehicles':
                if (data.vehicles) {
                    this.displayNearbyVehicles(data.vehicles);
                }
                break;
            default:
                console.log('Unknown WebSocket message type:', data.type);
        }
    }
    
    updateVehiclePositions(vehicles) {
        // Clear existing vehicle markers
        this.vehicleMarkers.forEach(marker => {
            this.map.removeLayer(marker);
        });
        this.vehicleMarkers = [];
        
        // Add new vehicle markers
        if (vehicles && vehicles.length > 0) {
            vehicles.forEach(vehicle => {
            const icon = L.divIcon({
                className: 'vehicle-marker',
                html: `<div class="vehicle-icon ${vehicle.route_id.includes('BLUE') || vehicle.route_id.includes('PURPLE') ? 'metro' : 'bus'}">
                        <span class="vehicle-id">${vehicle.route_id}</span>
                       </div>`,
                iconSize: [40, 20],
                iconAnchor: [20, 10]
            });
            
            const marker = L.marker([vehicle.latitude, vehicle.longitude], { icon })
                .bindPopup(`
                    <div class="vehicle-popup">
                        <h4>Vehicle ${vehicle.vehicle_id}</h4>
                        <p><strong>Route:</strong> ${vehicle.route_id}</p>
                        <p><strong>Occupancy:</strong> ${vehicle.occupancy_status.replace(/_/g, ' ')}</p>
                        <p><strong>Delay:</strong> ${vehicle.delay_minutes > 0 ? '+' : ''}${vehicle.delay_minutes} min</p>
                    </div>
                `)
                .addTo(this.map);
            
            this.vehicleMarkers.push(marker);
            });
        }
    }
    
    updateFareInformation(fares) {
        // Update fare display if currently showing results
        const resultsDiv = document.getElementById('results');
        if (resultsDiv && resultsDiv.style.display !== 'none') {
            // Re-calculate and display updated fares
            if (this.currentRoute) {
                this.displayFareResults(fares);
            }
        }
    }
    
    displayFareResults(fareResults) {
        const resultsDiv = document.getElementById('results');
        const contentDiv = document.getElementById('pricing-content');
        
        // Store fare results for detailed view
        this.fareResults = fareResults;
        
        // Show transit selection buttons
        let html = '<div class="transit-selection">';
        html += '<h3>🚌 Select Transit Option</h3>';
        html += '<div class="transit-buttons">';
        
        // Create selection buttons for each transport mode
        Object.entries(fareResults).forEach(([mode, fareData]) => {
            const realTimeFare = fareData.real_time_fare || fareData;
            const transportIcon = this.getTransportIcon(mode);
            const transportName = this.getTransportName(mode);
            
            html += `
                <button class="transit-button" onclick="app.showSelectedTransitDetails('${mode}')">
                    <div class="transit-button-icon">${transportIcon}</div>
                    <div class="transit-button-content">
                        <div class="transit-button-name">${transportName}</div>
                        <div class="transit-button-fare">₹${this.formatFare(realTimeFare.total_fare)}</div>
                    </div>
                </button>
            `;
        });
        
        html += '</div></div>';
        
        // Add detailed view container
        html += '<div id="transport-details" class="transport-details-container" style="display: none;"></div>';
        
        contentDiv.innerHTML = html;
        resultsDiv.style.display = 'block';
    }
    
    getTransportIcon(mode) {
        const icons = {
            'bus': '🚌',
            'metro': '🚇',
            'taxi': '🚕',
            'auto': '🛺'
        };
        return icons[mode] || '🚗';
    }
    
    getTransportName(mode) {
        const names = {
            'bus': 'Bus',
            'metro': 'Metro',
            'taxi': 'Taxi',
            'auto': 'Auto Rickshaw'
        };
        return names[mode] || mode.replace('_', ' ').toUpperCase();
    }
    
    showSelectedTransitDetails(mode) {
        const fareData = this.fareResults[mode];
        if (!fareData) return;
        
        const realTimeFare = fareData.real_time_fare || fareData;
        const routeInfo = fareData.route_info || {};
        const transportDetails = fareData.transport_details || {};
        
        const detailsContainer = document.getElementById('transport-details');
        const transportName = this.getTransportName(mode);
        const transportIcon = this.getTransportIcon(mode);
        
        const surgeInfo = realTimeFare.surge_multiplier > 1 ? 
            `<div class="surge-status active">Surge ${realTimeFare.surge_multiplier}x Active</div>` : '';
        
        let html = `
            <div class="transport-details-header">
                <button class="back-button" onclick="app.hideSelectedTransitDetails()">← Back to Options</button>
                <div class="transport-title">
                    <span class="transport-icon-large">${transportIcon}</span>
                    <h2>${transportName} Details</h2>
                </div>
            </div>
            
            <div class="transport-details-content">
                <div class="details-section fare-section">
                    <h3>💰 Fare Information</h3>
                    <div class="fare-breakdown-detailed">
                        <div class="fare-item">
                            <span class="fare-label">Total Fare</span>
                            <span class="fare-value total">₹${this.formatFare(realTimeFare.total_fare)}</span>
                        </div>
                        <div class="fare-item">
                            <span class="fare-label">Base Fare</span>
                            <span class="fare-value">₹${this.formatFare(realTimeFare.base_fare)}</span>
                        </div>
                    </div>
                    ${surgeInfo}
                </div>
                
                <div class="details-section journey-section">
                    <h3>🛣️ Journey Information</h3>
                    <div class="journey-stats">
                        <div class="stat-card">
                            <div class="stat-icon">📏</div>
                            <div class="stat-content">
                                <div class="stat-label">Distance</div>
                                <div class="stat-value">${this.formatDistance(routeInfo.road_distance_km || fareData.distance_km)} km</div>
                                <div class="stat-note">via roads</div>
                            </div>
                        </div>
                        
                        <div class="stat-card">
                            <div class="stat-icon">⏱️</div>
                            <div class="stat-content">
                                <div class="stat-label">Journey Time</div>
                                <div class="stat-value">${this.formatTime(routeInfo.journey_time_minutes)} min</div>
                                <div class="stat-note">estimated</div>
                            </div>
                        </div>
                        
                        ${routeInfo.departure_time ? `
                            <div class="stat-card">
                                <div class="stat-icon">🕐</div>
                                <div class="stat-content">
                                    <div class="stat-label">Departure</div>
                                    <div class="stat-value">${routeInfo.departure_time}</div>
                                </div>
                            </div>
                        ` : ''}
                        
                        ${routeInfo.arrival_time ? `
                            <div class="stat-card">
                                <div class="stat-icon">🏁</div>
                                <div class="stat-content">
                                    <div class="stat-label">Arrival</div>
                                    <div class="stat-value">${routeInfo.arrival_time}</div>
                                </div>
                            </div>
                        ` : ''}
                    </div>
                    
                    ${mode === 'metro' && (transportDetails.source_station || transportDetails.dest_station) ? `
                        <div class="metro-route-details">
                            <h4>🚇 Metro Route Details</h4>
                            <div class="metro-stations">
                                ${transportDetails.source_station ? `
                                    <div class="station-info">
                                        <div class="station-icon">🚉</div>
                                        <div class="station-content">
                                            <div class="station-label">Boarding Station</div>
                                            <div class="station-name">${transportDetails.source_station}</div>
                                            ${transportDetails.walking_to_metro_km ? `
                                                <div class="walking-distance">🚶 ${this.formatDistance(transportDetails.walking_to_metro_km)} km walk</div>
                                            ` : ''}
                                        </div>
                                    </div>
                                ` : ''}
                                
                                ${transportDetails.metro_distance_km ? `
                                    <div class="metro-journey">
                                        <div class="metro-line">
                                            <div class="metro-icon">🚇</div>
                                            <div class="metro-distance">${this.formatDistance(transportDetails.metro_distance_km)} km by metro</div>
                                        </div>
                                    </div>
                                ` : ''}
                                
                                ${transportDetails.dest_station ? `
                                    <div class="station-info">
                                        <div class="station-icon">🚉</div>
                                        <div class="station-content">
                                            <div class="station-label">Alighting Station</div>
                                            <div class="station-name">${transportDetails.dest_station}</div>
                                            ${transportDetails.walking_from_metro_km ? `
                                                <div class="walking-distance">🚶 ${this.formatDistance(transportDetails.walking_from_metro_km)} km walk</div>
                                            ` : ''}
                                        </div>
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    ` : ''}
                </div>
                
                <div class="details-section transport-section">
                    <h3>🚀 Transport Details</h3>
                    <div class="transport-specs">
                        ${transportDetails.average_speed_kmh ? `
                            <div class="spec-item">
                                <span class="spec-label">Average Speed</span>
                                <span class="spec-value">${transportDetails.average_speed_kmh} km/h</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.frequency_minutes ? `
                            <div class="spec-item">
                                <span class="spec-label">Service Frequency</span>
                                <span class="spec-value">Every ${transportDetails.frequency_minutes} minutes</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.comfort_level ? `
                            <div class="spec-item">
                                <span class="spec-label">Comfort Level</span>
                                <span class="spec-value">${transportDetails.comfort_level}</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.stops_estimated ? `
                            <div class="spec-item">
                                <span class="spec-label">Bus Stops</span>
                                <span class="spec-value">${transportDetails.stops_estimated} stops</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.stations_estimated ? `
                            <div class="spec-item">
                                <span class="spec-label">Metro Stations</span>
                                <span class="spec-value">${transportDetails.stations_estimated} stations</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.accessibility ? `
                            <div class="spec-item accessibility">
                                <span class="spec-label">♿ Accessibility</span>
                                <span class="spec-value">${transportDetails.accessibility}</span>
                            </div>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
        
        detailsContainer.innerHTML = html;
        detailsContainer.style.display = 'block';
        
        // Hide the transit selection
        document.querySelector('.transit-selection').style.display = 'none';
    }

    showTransportDetails(mode) {
        const fareData = this.fareResults[mode];
        if (!fareData) return;
        
        const realTimeFare = fareData.real_time_fare || fareData;
        const routeInfo = fareData.route_info || {};
        const transportDetails = fareData.transport_details || {};
        
        const detailsContainer = document.getElementById('transport-details');
        const transportName = this.getTransportName(mode);
        const transportIcon = this.getTransportIcon(mode);
        
        const surgeInfo = realTimeFare.surge_multiplier > 1 ? 
            `<div class="surge-status active">Surge ${realTimeFare.surge_multiplier}x Active</div>` : '';
        
        let html = `
            <div class="transport-details-header">
                <button class="back-button" onclick="app.hideTransportDetails()">← Back to Options</button>
                <div class="transport-title">
                    <span class="transport-icon-large">${transportIcon}</span>
                    <h2>${transportName} Details</h2>
                </div>
            </div>
            
            <div class="transport-details-content">
                <div class="details-section fare-section">
                    <h3>💰 Fare Information</h3>
                    <div class="fare-breakdown-detailed">
                        <div class="fare-item">
                            <span class="fare-label">Total Fare</span>
                            <span class="fare-value total">₹${this.formatFare(realTimeFare.total_fare)}</span>
                        </div>
                        <div class="fare-item">
                            <span class="fare-label">Base Fare</span>
                            <span class="fare-value">₹${this.formatFare(realTimeFare.base_fare)}</span>
                        </div>
                    </div>
                    ${surgeInfo}
                </div>
                
                <div class="details-section journey-section">
                    <h3>🛣️ Journey Information</h3>
                    <div class="journey-stats">
                        <div class="stat-card">
                            <div class="stat-icon">📏</div>
                            <div class="stat-content">
                                <div class="stat-label">Distance</div>
                                <div class="stat-value">${this.formatDistance(routeInfo.road_distance_km || fareData.distance_km)} km</div>
                                <div class="stat-note">via roads</div>
                            </div>
                        </div>
                        
                        <div class="stat-card">
                            <div class="stat-icon">⏱️</div>
                            <div class="stat-content">
                                <div class="stat-label">Journey Time</div>
                                <div class="stat-value">${this.formatTime(routeInfo.journey_time_minutes)} min</div>
                                <div class="stat-note">estimated</div>
                            </div>
                        </div>
                        
                        ${routeInfo.departure_time ? `
                            <div class="stat-card">
                                <div class="stat-icon">🕐</div>
                                <div class="stat-content">
                                    <div class="stat-label">Departure</div>
                                    <div class="stat-value">${routeInfo.departure_time}</div>
                                </div>
                            </div>
                        ` : ''}
                        
                        ${routeInfo.arrival_time ? `
                            <div class="stat-card">
                                <div class="stat-icon">🏁</div>
                                <div class="stat-content">
                                    <div class="stat-label">Arrival</div>
                                    <div class="stat-value">${routeInfo.arrival_time}</div>
                                </div>
                            </div>
                        ` : ''}
                    </div>
                </div>
                
                <div class="details-section transport-section">
                    <h3>🚀 Transport Details</h3>
                    <div class="transport-specs">
                        ${transportDetails.average_speed_kmh ? `
                            <div class="spec-item">
                                <span class="spec-label">Average Speed</span>
                                <span class="spec-value">${transportDetails.average_speed_kmh} km/h</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.frequency_minutes ? `
                            <div class="spec-item">
                                <span class="spec-label">Service Frequency</span>
                                <span class="spec-value">Every ${transportDetails.frequency_minutes} minutes</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.comfort_level ? `
                            <div class="spec-item">
                                <span class="spec-label">Comfort Level</span>
                                <span class="spec-value">${transportDetails.comfort_level}</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.stops_estimated ? `
                            <div class="spec-item">
                                <span class="spec-label">Bus Stops</span>
                                <span class="spec-value">${transportDetails.stops_estimated} stops</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.stations_estimated ? `
                            <div class="spec-item">
                                <span class="spec-label">Metro Stations</span>
                                <span class="spec-value">${transportDetails.stations_estimated} stations</span>
                            </div>
                        ` : ''}
                        
                        ${transportDetails.accessibility ? `
                            <div class="spec-item accessibility">
                                <span class="spec-label">♿ Accessibility</span>
                                <span class="spec-value">${transportDetails.accessibility}</span>
                            </div>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
        
        detailsContainer.innerHTML = html;
        detailsContainer.style.display = 'block';
        
        // Hide the transport options
        document.querySelector('.transport-options').style.display = 'none';
    }
    
    hideSelectedTransitDetails() {
        document.getElementById('transport-details').style.display = 'none';
        document.querySelector('.transit-selection').style.display = 'block';
    }

    hideTransportDetails() {
        document.getElementById('transport-details').style.display = 'none';
        const transportOptions = document.querySelector('.transport-options');
        if (transportOptions) {
            transportOptions.style.display = 'block';
        }
    }
    
    showNotification(message, type = 'info', duration = 5000) {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.innerHTML = `
            <span class="notification-message">${message}</span>
            <button class="notification-close" onclick="this.parentElement.remove()">×</button>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto-remove after duration
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
}

// Utility functions
function getCurrentLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const latlng = L.latLng(position.coords.latitude, position.coords.longitude);
                app.setSourceLocation(latlng);
                app.map.setView(latlng, 15);
            },
            (error) => {
                alert('Unable to get your location. Please select manually on the map.');
            }
        );
    } else {
        alert('Geolocation is not supported by this browser.');
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
    if (app.routeLine) {
        app.map.removeLayer(app.routeLine);
        app.routeLine = null;
    }
    
    document.getElementById('source').value = '';
    document.getElementById('destination').value = '';
    document.getElementById('results').style.display = 'none';
    
    app.isSelectingSource = true;
}

// Initialize the application when the page loads
let app;
document.addEventListener('DOMContentLoaded', () => {
    app = new BangaloreTransitApp();
});