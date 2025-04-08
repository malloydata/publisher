import { Home } from "@malloy-publisher/sdk";
import { useNavigate } from "react-router-dom";
interface HomePageProps {
   server?: string;
}

export function HomePage({ server }: HomePageProps) {
   const navigate = useNavigate();
   return <Home server={server} navigate={navigate} />;
}
