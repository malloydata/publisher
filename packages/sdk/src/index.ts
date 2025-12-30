export * from "./components";
export { default as ConnectionExplorer } from "./components/Project/ConnectionExplorer";
export { useServer } from "./components/ServerProvider";
export * from "./hooks";
export { useRawQueryData } from "./hooks/useRawQueryData";
export * from "./utils/formatting";
import axios from "axios";

// There's a bug in the OpenAPI generator that causes it to ignore baseURL in the
// axios request if defaults.baseURL is not set.
axios.defaults.baseURL = "IfYouAreSeeingThis_baseURL_IsNotSet";
