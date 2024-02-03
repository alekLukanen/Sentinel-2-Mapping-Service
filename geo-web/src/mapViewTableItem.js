
import { useSelector } from "react-redux"
import Card from 'react-bootstrap/Card';
import Button from 'react-bootstrap/Button';
import Badge from 'react-bootstrap/Badge';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';

import { selectNDVIRasterByBoundaryId } from "./state/rasters.js"
import { selectBoundaryById } from "./state/boundaries.js"


export function MapViewTableItem({boundaryId, handleDeleteBoundary}) {
    const renderCloudTooltip = (props) => (
        <Tooltip id="cloud-tooltip" {...props}>
          When cloud cover is too high the map will become obscured. If cloud cover is more than 90% you may not see a map at all.
        </Tooltip>
      );

    const raster = useSelector(state => selectNDVIRasterByBoundaryId(state, boundaryId))
    const boundary = useSelector(state => selectBoundaryById(state, boundaryId))

    return (
        <Card>
            <Card.Body>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                        <h5>{boundary.name}</h5>
                        <p>Area: { boundary.acres.toFixed(1) } ac</p>
                    </div>
                    <Button
                        variant="danger"
                        
                        onClick={() => handleDeleteBoundary(boundaryId)}
                    >
                        X
                    </Button>
                </div>
                <hr></hr>
                <Badge className="Mapview-Badge" bg={ raster ? raster?.metaData?.rasterMean > 0.5 ? "success" : "warning" : "secondary"}>NDVI Average: { raster ? raster?.metaData?.rasterMean?.toFixed(3) : "Processing"}</Badge>
                <OverlayTrigger
                    placement="top"
                    delay={{ show: 250, hide: 400 }}
                    overlay={renderCloudTooltip}
                >
                    <Badge className="Mapview-Badge" bg={ raster ? raster?.metaData?.rasterPercentCoveredByClouds > 10 ? "danger" : "secondary" : "secondary"}>NDVI Cloud Cover: { raster ? raster?.metaData?.rasterPercentCoveredByClouds?.toFixed(1) + " %" : "Processing"}</Badge>
                </OverlayTrigger>
                <Badge className="Mapview-Badge" bg="secondary">NDVI Date: {raster ? (raster?.tileDates?.length ? new Date(raster.tileDates[0]).toLocaleDateString() : "missing date") : "processing"}</Badge>
            </Card.Body>
        </Card>
    )
}