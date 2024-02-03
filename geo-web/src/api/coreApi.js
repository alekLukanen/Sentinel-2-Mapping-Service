import {config} from '../config'
import axios from 'axios';


export async function loginRequest(username, password) {
    return axios.post(`${config[process.env.NODE_ENV].coreApiBaseUrl}/signin`, {
        name: username,
        password: password
    })
}


export async function signupRequest(username, password) {
    return axios.post(`${config[process.env.NODE_ENV].coreApiBaseUrl}/signup`, {
        name: username,
        password: password
    })
}


export async function boundariesRequest(auth) {
    return axios.get(`${config[process.env.NODE_ENV].coreApiBaseUrl}/boundary`, {
        headers: {
            Token: auth.accessToken
        }
    })
}

export const createBoundaryRequest = async (auth, newBoundary) => {
    const apiUrl = `${config[process.env.NODE_ENV].coreApiBaseUrl}/boundary`;
    const requestBody = {
      name: newBoundary.name,
      geometry: newBoundary.geometry.geometry,
    };
  
    const response = await axios.post(
        apiUrl, 
        requestBody, {
        headers: {
            Token: auth.accessToken
        }
    });
    return response;
}

export const deleteBoundaryRequest = async (auth, boundaryId) => {
    const apiUrl = `${config[process.env.NODE_ENV].coreApiBaseUrl}/boundary/${boundaryId}`;
    const response = await axios.delete(apiUrl, {
        headers: {
            Token: auth.accessToken
        }
    });
    return response;
}


export const boundaryRastersRequest = async (auth, boundaryId) => {
    const apiUrl = `${config[process.env.NODE_ENV].coreApiBaseUrl}/boundary/${boundaryId}/rasters`;
    const response = await axios.get(apiUrl, {
        headers: {
            Token: auth.accessToken
        }
    });
    return response;
}


export const rasterImageDataRequest = async (auth, rasterId) => {
    const apiUrl = `${config[process.env.NODE_ENV].coreApiBaseUrl}/raster/image/${rasterId}`;
    const response = await axios.get(apiUrl, {
        headers: {
            Token: auth.accessToken
        }
    });
    return response;
}