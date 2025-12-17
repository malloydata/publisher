import { Home, useRouterClickHandler } from "@malloy-publisher/sdk";

export default function HomePage() {
   const navigate = useRouterClickHandler();
   return <Home onClickProject={navigate} />;
}
