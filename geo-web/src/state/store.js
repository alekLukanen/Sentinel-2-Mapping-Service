import { configureStore } from '@reduxjs/toolkit'
import { combineReducers } from 'redux'

import auth from './login'
import boundaries from './boundaries'
import rasters from './rasters'

const reducer = combineReducers({
  auth, boundaries, rasters
})

const store = configureStore({
  reducer,
})

export default store