import './backgroundImage.css'
import './loginPage.css'
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';

import { useNavigate } from "react-router-dom";
import { useEffect, useState } from 'react';
import { useSelector, useDispatch } from 'react-redux';

import { fetchNewSignupAuth, selectUsername, selectAccessToken } from './state/login.js';


export function SignupPage() {

    const dispatch = useDispatch()
    const navigate = useNavigate()
    const username = useSelector(selectUsername)
    const accessToken = useSelector(selectAccessToken)

    const authStatus = useSelector(state => state.auth.status)

    const [usernameInput, setUsernameInput] = useState("")
    const [passwordInput, setPasswordInput] = useState("")
    const [passwordInput2, setPasswordInput2] = useState("")

    useEffect(() => {
        if (authStatus === "succeeded") {
            console.log("signup completed")
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
                        <Card.Header as="h5">Signup New User</Card.Header>
                        <Card.Body>
                            <Form autoComplete="new-password">
                                <Form.Group className="mb-3" controlId="formBasicUsername">
                                    <Form.Label>Username</Form.Label>
                                    <Form.Control 
                                        type="text" 
                                        placeholder="Enter username"
                                        value={usernameInput}
                                        onChange={el => setUsernameInput(el.target.value)}
                                    />
                                </Form.Group>

                                <Form.Group className="mb-3" controlId="formBasicPassword">
                                    <Form.Label>Password (must be 8 characters)</Form.Label>
                                    <Form.Control 
                                        type="password" 
                                        placeholder="Password"
                                        value={passwordInput}
                                        onChange={el => setPasswordInput(el.target.value)}
                                    />
                                </Form.Group>

                                <Form.Group className="mb-3" controlId="formBasicPassword">
                                    <Form.Label>Enter Password Again</Form.Label>
                                    <Form.Control 
                                        type="password" 
                                        placeholder="Password"
                                        value={passwordInput2}
                                        onChange={el => setPasswordInput2(el.target.value)}
                                    />
                                </Form.Group>

                                <Button variant="primary" disabled={!usernameInput.length || passwordInput.length < 8 || passwordInput !== passwordInput2} onClick={() => dispatch(fetchNewSignupAuth({user: usernameInput, pass: passwordInput}))}>
                                    Signup
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