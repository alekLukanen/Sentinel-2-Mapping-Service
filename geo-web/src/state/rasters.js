import { boundaryRastersRequest, rasterImageDataRequest } from '../api/coreApi';
import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';


// create a rasters slice using the imported request functions
const initialState = {
    rasters: [],
    status: 'idle',
    error: null,
}


export const rastersSlice = createSlice({
    name: 'rasters',
    initialState,
    reducers: {},
    extraReducers(builder) {
        builder
            .addCase(fetchBoundaryNDVIRaster.pending, (state, action) => {
                state.status = 'loading';
            })
            .addCase(fetchBoundaryNDVIRaster.fulfilled, (state, action) => {
                state.status = 'succeeded';
                // remove the old raster from the state and add the new one
                if (action.payload !== undefined) {
                    state.rasters = state.rasters.filter(
                        (raster) => raster.id !== action.payload.id
                    );
                    state.rasters.push(action.payload);
                }
            })
            .addCase(fetchBoundaryNDVIRaster.rejected, (state, action) => {
                state.status = 'failed';
                state.error = action.error.message;
            })
        }
    }
)


export const fetchBoundaryNDVIRaster = createAsyncThunk(
    'rasters/fetchBoundaryNDVIRaster',
    async ({ auth, boundaryId }, thunkAPI) => {
      try {
        const boundaryRasters = await boundaryRastersRequest(auth, boundaryId);
        const ndviRaster = boundaryRasters.data.rasters.find(el => el.type === 'NDVI_MAP');
        if (ndviRaster !== undefined) {
            const ndviRasterImageData = await rasterImageDataRequest(auth, ndviRaster.id);
            ndviRaster.imageData = ndviRasterImageData.data;
            return ndviRaster;
        } else {
            return undefined;
        }
      } catch (error) {
        console.log(error)
        return thunkAPI.rejectWithValue(error.message);
      }
    }
)

export const selectRasters = state => state.rasters.rasters;

export const selectNDVIRasterByBoundaryId = (state, boundaryId) => {
    const ndviRaster = state.rasters.rasters.find(el => el.boundaryId === boundaryId && el.type === 'NDVI_MAP');
    return ndviRaster;
}

export const { rasterActions } = rastersSlice;

export default rastersSlice.reducer;
