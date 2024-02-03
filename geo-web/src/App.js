

import {
  createBrowserRouter,
  RouterProvider,
} from "react-router-dom";

import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';
import {MapView} from './mapView.js'
import {HomePage} from './homePage.js'
import {ErrorPage} from './errorPage.jsx'
import {LoginPage} from './loginPage.js'
import { SignupPage } from "./signupPage";

const router = createBrowserRouter([
  {
    path: "/",
    element: <HomePage />,
    errorElement: <ErrorPage />,
  },
  {
    path: "/r/login",
    element: <LoginPage />,
  },
  {
    path: "/r/signup",
    element: <SignupPage />,
  },
  {
    path: "/r/map",
    element: <MapView />,
  }
]);


function App() {
  document.title = "GeoWeb"
  return (
    <div className="App">
      <RouterProvider router={router} />
    </div>
  );
}

export default App;
