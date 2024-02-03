import './backgroundImage.css'
import './homePage.css'
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';

export function HomePage() {
    return (
        <div id='HomePage-Image'>
            <Container className='HomePage-Container'>
                <Row className='HomePage-Row'>
                    <Col sm={{ span: 8, offset: 2 }}>
                        <div className='HomePage'>
                            <header className="HomePage-Header">
                                <h1>NDVI Map Generator</h1>
                            </header>
                            <hr/>
                            <p>A simple tool for generating NDVI maps within a boundary extent</p>
                            <p>Sentinel-2 Cloud-Optimized GeoTIFFs sourced from <a href="https://registry.opendata.aws/sentinel-2-l2a-cogs" target="_blank">AWS Open Data</a></p>
                        </div>
                    </Col>
                    <Col className="HomePage-Header" md={{ span: 6, offset: 3}}>
                        <Button className="Account-Button" variant="outline-primary" href='/r/login'>Login</Button>
                        <Button className="Account-Button" variant="outline-primary" href='/r/signup'>Signup</Button>
                    </Col>
                </Row>
            </Container>
        </div>
    )
}