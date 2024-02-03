import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Table from 'react-bootstrap/Table';
import { MapContainer } from 'react-leaflet/MapContainer'
import { GeoJSON, Tooltip } from 'react-leaflet'
import { TileLayer, FeatureGroup, ImageOverlay } from 'react-leaflet';
import { EditControl } from "react-leaflet-draw"
import { geoJSON } from 'leaflet';
import Card from 'react-bootstrap/Card';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import 'leaflet/dist/leaflet.css'
import 'leaflet-draw/dist/leaflet.draw.css';
import * as d3 from 'd3';
import './mapView.css'

import { useSelector, useDispatch } from 'react-redux'
import { useEffect, useState } from 'react'

import {selectAuth} from './state/login.js'
import {fetchBoundaries, selectBoundaries, addNewBoundary, deleteBoundary} from './state/boundaries.js'
import {fetchBoundaryNDVIRaster, selectRasters} from './state/rasters.js'
import { MapViewTableItem } from './mapViewTableItem'



const defaultBounds = [
    [43.4994, -97.2392], // Southwest corner (latitude, longitude)
    [49.3844, -89.4834]  // Northeast corner (latitude, longitude)
  ];

export function MapView() {

    const dispatch = useDispatch()
    const auth = useSelector(selectAuth)
    const boundariesStatus = useSelector(state => state.boundaries.status)
    const boundaries = useSelector(selectBoundaries)
    const rasters = useSelector(selectRasters)

    const [editControlVisible, setEditControlVisible] = useState(false);
    const toggleEditControls = () => {
        setEditControlVisible((prevVisible) => !prevVisible);
      };

    const [newBoundary, setNewBoundary] = useState({ name: '', geometry: null });

    const [bounds, setBounds] = useState(defaultBounds)
    const [boundsComputed, setBoundsComputed] = useState(false)
    const [mapsRequested, setMapsRequested] = useState(false)
    const [showHelp, setShowHelp] = useState(false);

    const [svgRef, setSvgRef] = useState(null);

    useEffect(() => {

        const svg = d3.select(svgRef);

        const colorScale = d3.scaleSequential(d3.interpolateRdYlGn).domain([1, -1]);

        const colorBarData = d3.range(-1.0, 1.01, 1/50.); // Create a range of values from -1 to 1
        const colorBarNumbersData = d3.range(1.0, -1.50, -1/2.); // Create a range

        svg.selectAll('rect')
        .data(colorBarData)
        .enter()
        .append('rect')
        .attr('y', (d, i) => i * 2) // Adjust the positioning based on your preferences
        .attr('x', 0)
        .attr('width', 50) // Adjust the width of color steps
        .attr('height', 2) // Adjust the height of the color bar
        .attr('fill', colorScale);

        svg.selectAll('text')
            .data(colorBarNumbersData)
            .enter()
            .append('text')
            .attr('x', 55) // Adjust the x-position of the labels
            .attr('y', (d, i) => Math.max(10, i * 50)) // Adjust the y-position of the labels
            .text(d => d.toFixed(2)); // Display the value with 2 decimal places


    }, [svgRef]);

    useEffect(() => {
        if (boundariesStatus === "idle") {
            dispatch(fetchBoundaries(auth))
        }
    }, [dispatch, auth, boundariesStatus])

    useEffect(() => {
        if (boundaries.length && !boundsComputed) {
            const allBoundaries = boundaries.map((boundary) => boundary.geometry)
            const boundsValue = geoJSON(allBoundaries).getBounds()
            setBounds(boundsValue)
        }
        if (boundariesStatus === "succeeded") {
            setBoundsComputed(true)
        }
    }, [boundaries, boundariesStatus, boundsComputed])

    useEffect(() => {
        if (boundaries.length && !mapsRequested) {
            boundaries.forEach((boundary) => {
                dispatch(fetchBoundaryNDVIRaster({ auth: auth, boundaryId: boundary.id }))
            })
            setMapsRequested(true)
        }
    }, [dispatch, auth, mapsRequested, boundaries])

    const handleDrawCreated = (e) => {
        const { layerType, layer } = e;
        if (layerType === 'marker') {
          // For markers, add a popup with a label
          layer.bindPopup('A new marker!');
        }
        layer.addTo(e.target);

        setNewBoundary({ name: '', geometry: layer.toGeoJSON() });
      };

    const handleSaveBoundary = () => {
        if (newBoundary.name.trim() !== '') {
            // Add the new boundary to the list
            dispatch(addNewBoundary({
                auth: auth,
                newBoundary: { ...newBoundary },
            }));
            // Clear the new boundary state
            setNewBoundary({ name: '', geometry: null });
            // Hide the edit controls
            setEditControlVisible(false);
        }
    };

    const handleDeleteBoundary = (boundaryId) => {
        // Dispatch an action to delete the boundary
        dispatch(deleteBoundary({ auth: auth, boundaryId: boundaryId }));
    };

    if (!boundsComputed) {
        return <div className="loader">Loading...</div>
    } else {
        return (
            <Container className='map-container'>
                <Row className="Mapview-Row">
                    <Col sm="9" className="MapView-Col">
                        <MapContainer height="500px" zoom={10} scrollWheelZoom={true} bounds={bounds}>
                            {editControlVisible && (
                                <FeatureGroup>
                                    <EditControl
                                        position='topright'
                                        onCreated={handleDrawCreated}
                                        draw={{
                                            polygon: true,
                                            polyline: false,
                                            rectangle: false,
                                            circle: false,
                                            marker: false,
                                            circlemarker: false
                                        }}
                                    />
                                </FeatureGroup>
                                )
                            }

                            <div className='Mapview-Legend'>
                                <p style={{fontSize: "1rem"}}>Legend</p>
                                <svg ref={setSvgRef} width="90" height="200"></svg>
                            </div>

                            <TileLayer
                                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                            />

                            <GeoJSON
                                style={{color: "green", fillOpacity: 0.0, weight: 2.0}}
                                data={{
                                    "coordinates": [
                                      [
                                        [
                                          -96.0,
                                          48.0
                                        ],
                                        [
                                          -96.0,
                                          40.0
                                        ],
                                        [
                                          -90.0,
                                          40
                                        ],
                                        [
                                          -90.0,
                                          48.0
                                        ],
                                        [
                                          -96.0,
                                          48.0
                                        ]
                                      ]
                                    ],
                                    "type": "Polygon"
                                  }}
                                  />

                            { boundaries.map(
                                (el, i) => 
                                <GeoJSON 
                                    key={el.id}
                                    style={{color: 'red', fillColor: 'red', fillOpacity: !rasters.some(raster => raster.boundaryId === el.id) ? 0.25 : 0.0, weight: 1.0}}
                                    data={el.geometry}
                                ><Tooltip direction="bottom">{el.name}</Tooltip>
                                </GeoJSON>
                                )
                            }
                            { rasters && rasters.map(
                                (el, i) => boundaries.find(boundary => boundary.id === el.boundaryId) ? <ImageOverlay key={el.id} url={el.imageData} bounds={el.metaData.imageBounds} opacity={1.0} />: undefined)
                            }
                        </MapContainer>
                    </Col>
                    <Col sm="3" className="MapView-Col">
                        <div className="Maview-Table-Container">
                        <Table className="Mapview-Table">
                            <thead>
                                <tr>
                                    <th>
                                        <Card>
                                            <Card.Body>
                                                <h4>NDVI Map Boundaries</h4>
                                                <hr></hr>
                                                <Button onClick={toggleEditControls} variant="primary">Create New Boundary</Button>
                                                <Button onClick={() => setShowHelp(!showHelp)} variant='secondary'>Show Help</Button>
                                                {showHelp && (
                                                <ul>
                                                    <li>The green box represents UTM zone 15T and is the only supported region for which maps will be generated</li>
                                                    <li>Max of 10 boundaries can be saved</li>
                                                    <li>Max of 2500 acres per boundary</li>
                                                    <li>Map proessing takes a few minutes and the page must be refreshed to get new maps.</li>
                                                </ul>
                                                )}
                                            </Card.Body>
                                        </Card>

                                        {editControlVisible && (
                                            <Card>
                                                <Card.Body>
                                                
                                                <Form.Check // prettier-ignore
                                                    type="checkbox"
                                                    label="Draw boundary with tools found in upper left of map (area must be less than 2500 acres)"
                                                    checked={Boolean(newBoundary.geometry)}
                                                    readOnly
                                                />
                                                <Form.Check
                                                    type="checkbox"
                                                    label="Set Boundary Name"
                                                    checked={Boolean(newBoundary.name)}
                                                    readOnly
                                                />
                                                <input
                                                    type="text"
                                                    placeholder="Boundary Name"
                                                    value={newBoundary.name}
                                                    maxLength={50}
                                                    onChange={(e) => setNewBoundary({ ...newBoundary, name: e.target.value })}
                                                />
                                                <Button variant="primary" disabled={!newBoundary.geometry || !newBoundary.name} onClick={handleSaveBoundary}>Save</Button>
                                                    
                                                </Card.Body>

                                            </Card>
                                        )}
                                        
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                            {boundaries.map((el, i) => (
                                <tr key={el.id}>
                                <td>
                                    <MapViewTableItem boundaryId={el.id} handleDeleteBoundary={() => handleDeleteBoundary(el.id)}/>
                                </td>
                                </tr>
                            ))}
                            </tbody>
                        </Table>
                        </div>
                    </Col>
                </Row>
            </Container>
        );
    }
}

