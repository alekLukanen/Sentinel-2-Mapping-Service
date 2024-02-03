import './backgroundImage.css'
import './loginPage.css'
import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'
import Button from 'react-bootstrap/Button'
import Card from 'react-bootstrap/Card'
import Form from 'react-bootstrap/Form'
import { useNavigate } from "react-router-dom";

import { useEffect, useState } from 'react'
import { useSelector, useDispatch } from 'react-redux'

import {fetchAuth, selectUsername, selectAccessToken} from './state/login.js'


export function LoginPage() {
    const dispatch = useDispatch()
    const navigate = useNavigate()
    const username = useSelector(selectUsername)
    const accessToken = useSelector(selectAccessToken)

    const authStatus = useSelector(state => state.auth.status)

    const [usernameInput, setUsernameInput] = useState("")
    const [passwordInput, setPasswordInput] = useState("")


    useEffect(() => {
        if (authStatus === "succeeded") {
            console.log("login completed")
            navigate("/r/map")
        }
    }, [authStatus, accessToken, username, navigate])

    return (
        <div id='HomePage-Image'>
            <Container className='LoginPage-Container'>
                <Row className='LoginPage-Row'>
                    <Col md={{ span: 6, offset: 3 }}>
                        <div className='LoginPage'>
                            <Card>
                            <Card.Header as="h5">Login</Card.Header>
                            <Card.Body>
                                <Form>
                                    <Form.Group className="mb-3" controlId="formBasicUsername">
                                        <Form.Label>Username</Form.Label>
                                        <Form.Control type="text" placeholder="Enter username" value={usernameInput} onChange={el => setUsernameInput(el.target.value)}/>
                                    </Form.Group>

                                    <Form.Group className="mb-3" controlId="formBasicPassword">
                                        <Form.Label>Password</Form.Label>
                                        <Form.Control type="password" placeholder="Password" values={passwordInput} onChange={el => setPasswordInput(el.target.value)}/>
                                    </Form.Group>
                                    <Button variant="primary" onClick={() => dispatch(fetchAuth({user: usernameInput, pass: passwordInput}))}>
                                        Login
                                    </Button>
                                </Form>
                            </Card.Body>
                            </Card>
                        </div>
                    </Col>
                </Row>
            </Container>
        </div>
    )
}