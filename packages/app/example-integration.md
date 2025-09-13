# Integrating Malloy Publisher App

This guide shows how to use the bundled Malloy Publisher App in another React project.

## 1. Install in Your Project

```bash
# If using npm
npm install @malloy-publisher/app

# If using bun
bun add @malloy-publisher/app
```

## 3. Create Wrapper Component
In this example we demonstrate wrapping the App with a small
header and Auth0 authentication.

```tsx
// src/AuthenticatedMalloyApp.tsx
import React from 'react';
import { useAuth0, withAuthenticationRequired } from '@auth0/auth0-react';
import { MalloyPublisherApp, MalloyPublisherAppProps, createMalloyRouter } from '@malloy-publisher/app';
import { RouterProvider } from 'react-router-dom';
// You must manually import the styles for the MalloyExplorer, otherwise it 
// will render very weirds
import "@malloy-publisher/sdk/malloy-explorer.css";

interface AuthenticatedMalloyAppProps extends Omit<MalloyPublisherAppProps, 'server'> {
  serverUrl?: string;
}

const AuthenticatedMalloyAppComponent: React.FC<AuthenticatedMalloyAppProps> = ({ 
  serverUrl, 
  ...props 
}) => {
  const { getAccessTokenSilently } = useAuth0();
  const [accessToken, setAccessToken] = React.useState<string | null>(null);

  React.useEffect(() => {
    const getToken = async () => {
      try {
        const token = await getAccessTokenSilently();
        setAccessToken(token);
      } catch (error) {
        console.error('Error getting access token:', error);
      }
    };
    
    getToken();
  }, [getAccessTokenSilently]);

  // Pass the authenticated server URL with token
  const authenticatedServer = React.useMemo(() => {
    if (!serverUrl || !accessToken) return undefined;
    
    // You might need to modify this based on how your server expects the token
    return `${serverUrl}?access_token=${accessToken}`;
  }, [serverUrl, accessToken]);

  if (!accessToken) {
    return <div>Loading...</div>;
  }

  return (
    <MalloyPublisherApp 
      server={authenticatedServer}
      {...props}
    />
  );
};

// Wrap with Auth0 authentication requirement
export const AuthenticatedMalloyApp = withAuthenticationRequired(
  AuthenticatedMalloyAppComponent,
  {
    onRedirecting: () => <div>Redirecting to login...</div>,
  }
);
```

## 4. Understanding the Mutable Prop

The `mutable` prop is passed to the `ServerProvider` and controls whether users can create, edit, or delete projects and packages:

- `mutable={true}` (default): Users can manage projects and packages
- `mutable={false}`: Users can only view and explore existing projects and packages

This is useful for:
- **Read-only deployments**: When you want to provide access to data without allowing modifications
- **Production environments**: Where you want to prevent accidental changes
- **Embedded applications**: Where the parent application manages the data model

## 5. Setup Auth0 Provider in Your Main App

```tsx
// src/App.tsx
import React from 'react';
import { Auth0Provider } from '@auth0/auth0-react';
import { BrowserRouter } from 'react-router-dom';
import { AuthenticatedMalloyApp } from './AuthenticatedMalloyApp';

const App: React.FC = () => {
  return (
    <Auth0Provider
      domain="your-auth0-domain.auth0.com"
      clientId="your-auth0-client-id"
      authorizationParams={{
        redirect_uri: window.location.origin,
        audience: "your-api-audience", // if you have an API
        scope: "openid profile email"
      }}
    >
      <BrowserRouter>
        <div className="App">
          <header>
            <h1>My App with Malloy Publisher</h1>
          </header>
          
          <AuthenticatedMalloyApp 
            serverUrl="https://your-malloy-server.com/api/v0"
            basePath="/malloy"
            mutable={true} // Set to false to disable project/package management
          />
        </div>
      </BrowserRouter>
    </Auth0Provider>
  );
};

export default App;
```
