
import { loginRequest, signupRequest } from '../api/coreApi'
import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'


function saveToLocalStorage(key, state) {
    try {
      const serialisedState = JSON.stringify(state);
      localStorage.setItem(key, serialisedState);
    } catch (e) {
      console.warn(e);
    }
}

function loadFromLocalStorage(key) {
    try {
      const serialisedState = localStorage.getItem(key);
      if (serialisedState === null) return undefined;
      return JSON.parse(serialisedState);
    } catch (e) {
      console.warn(e);
      return {};
    }
}


const initialState = {
    username: null,
    accessToken: null,
    status: 'idle',
    error: null,
    ...loadFromLocalStorage("auth")
}

export const authSlice = createSlice({
    name: 'auth',
    initialState,
    reducers: {},
    extraReducers(builder) {
        builder
            .addCase(fetchAuth.pending, (state, action) => {
                state.status = 'loading'
            })
            .addCase(fetchAuth.fulfilled, (state, action) => {
                state.status = 'succeeded'
                state.username = action.payload.user
                state.accessToken = action.payload.responseData
                saveToLocalStorage("auth", {username: state.username, accessToken: state.accessToken})
            })
            .addCase(fetchAuth.rejected, (state, action) => {
                state.status = 'failed'
                state.error = action.error.message
            })
            .addCase(fetchNewSignupAuth.pending, (state, action) => {
                state.status = 'loading'
            })
            .addCase(fetchNewSignupAuth.fulfilled, (state, action) => {
                state.status = 'succeeded'
                state.username = action.payload.user
                state.accessToken = action.payload.responseData
                saveToLocalStorage("auth", {username: state.username, accessToken: state.accessToken})
            })
            .addCase(fetchNewSignupAuth.rejected, (state, action) => {
                state.status = 'failed'
                state.error = action.error.message
            })
    }
})

export const fetchAuth = createAsyncThunk('auth/fetchAuth', async (args) => {
    const {user, pass} = args
    const response = await loginRequest(user, pass)
    return {
        user: user,
        responseData: response.data
    }
})

export const fetchNewSignupAuth = createAsyncThunk('auth/fetchNewSignupAuth', async (args) => {
    const {user, pass} = args
    const response = await signupRequest(user, pass)
    return {
        user: user,
        responseData: response.data
    }
})

export const selectUsername = state => state.auth.username
export const selectAccessToken = state => state.auth.accessToken
export const selectAuth = state => state.auth

// Action creators are generated for each case reducer function
export const { loginUser } = authSlice.actions

export default authSlice.reducer