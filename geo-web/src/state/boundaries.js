import { boundariesRequest, deleteBoundaryRequest, createBoundaryRequest } from '../api/coreApi'
import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'


const initialState = {
    boundaries: [],
    status: 'idle',
    error: null,
}


export const boundariesSlice = createSlice({
    name: 'boundaries',
    initialState,
    reducers: {},
    extraReducers(builder) {
      builder
        .addCase(fetchBoundaries.pending, (state, action) => {
          state.status = 'loading'
        })
        .addCase(fetchBoundaries.fulfilled, (state, action) => {
          state.status = 'succeeded'
          state.boundaries = action.payload;
        })
        .addCase(fetchBoundaries.rejected, (state, action) => {
          state.status = 'failed'
          state.error = action.error.message;
        })
        .addCase(addNewBoundary.pending, (state, action) => {
          state.status = 'loading'
        })
        .addCase(addNewBoundary.fulfilled, (state, action) => {
          state.status = 'succeeded'
          state.boundaries.push(action.payload);
        })
        .addCase(addNewBoundary.rejected, (state, action) => {
          state.status = 'failed'
          state.error = action.error.message;
        })
        .addCase(deleteBoundary.pending, (state, action) => {
          state.status = 'loading'
        })
        .addCase(deleteBoundary.fulfilled, (state, action) => {
          state.status = 'succeeded'
          state.boundaries = state.boundaries.filter(
            (boundary) => boundary.id !== action.payload.id
          )
        })
    },
  });

export const fetchBoundaries = createAsyncThunk('boundaries/fetchBoundaries', async (auth, thunkAPI) => {
    try {
        const response = await boundariesRequest(auth)
        const boundaries = response.data

        return boundaries

    } catch (error) {
        console.log(error)
        return thunkAPI.rejectWithValue(error.message)
    }
})

export const addNewBoundary = createAsyncThunk(
    'boundaries/addNewBoundary',
    async ({ auth, newBoundary }, thunkAPI) => {
        try {
            const response = await createBoundaryRequest(auth, newBoundary)
            return response.data
        } catch (error) {
            console.log(error);
            return thunkAPI.rejectWithValue(error.message)
        }
    }
);

export const deleteBoundary = createAsyncThunk(
    'boundaries/deleteBoundary',
    async ({ auth, boundaryId }, thunkAPI) => {
      try {
        const response = await deleteBoundaryRequest(auth, boundaryId)
        if (response.status === 204) { 
          return {id: boundaryId}
        } else {
          return thunkAPI.rejectWithValue("bad status code: " + response.status)
        }
      } catch (error) {
        console.log(error)
        return thunkAPI.rejectWithValue(error.message)
      }
    }
  );


export const selectBoundaries = state => state.boundaries.boundaries

export const selectBoundaryById = (state, boundaryId) => state.boundaries.boundaries.find(boundary => boundary.id === boundaryId)

// Action creators are generated for each case reducer function
export const { boundariesActions } = boundariesSlice.actions

export default boundariesSlice.reducer