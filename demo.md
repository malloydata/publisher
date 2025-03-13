# Steps to Demo Publisher

## Setup Publisher

Follow the steps in the main readme to setup, build and run the publisher.

If you get a "permission denied" error when running `npm install`, try the following:

```bash
sudo chown -R 501:20 ~/.npm
```

Open the publisher in your browser (likely at `http://localhost:4000`).

## Create a Simple React App to Embed an Analysis

From outside of the publisher repo, run:

```bash
npx create-react-app embed-test
cd embed-test
npm install react@18.3.1 react-dom@18.3.1
npm install @malloy-publisher/sdk
```

Next replace the contents of `src/App.js` with the following:

```jsx
import React from "react";
import { QueryResult } from "@malloy-publisher/sdk";

const RUN_EMBED = true;

function App() {
    const accessToken = null;

    if (RUN_EMBED) {
        return (
            <QueryResult
                server="http://localhost:4000/api/v0"
                accessToken={accessToken}
                projectName="home"
                packageName="imdb"
                modelPath="genre_matrix.malloynb"
                query={`
                    run: movies -> {
                        where: genre2 != genre
                        group_by: genre2
                        aggregate: title_count
                        nest: genre is {
                            group_by: genre, genre2
                            nest: title_list + {limit: 7}
                            nest: top_directors
                            nest: top_actors
                        }
                    }
                `}
            />
        );
    } else {
        return (
            <QueryResult
                server="http://localhost:4000/api/v0"
                accessToken={accessToken}
                projectName="home"
                packageName="ecommerce"
                modelPath="ecommerce.malloy"
                sourceName="order_items"
                queryName="top_categories"
            />
        );
    }
}

export default App;
```

Then start the app with `npm start` and open it in your browser (likely at `http://localhost:3000`).

## Misc Debugging Notes

**NOTE: doing a local install of the publisher SDK is not working right now for some reason.**

- If you get a "permission denied" error when running `npm install` for publisher, try the following `sudo chown -R 501:20 ~/.npm`.
- If making changes to the publisher server, you need to rebuild and restart it.
- If you want to make sure you're using the latest version of the sdk, it's best to install it locally using `npm install /path/to/publisher/packages/sdk`.
- If you want to make changes to the sdk and have them picked up, you can link the sdk locally using `npm link` in the sdk directory and `npm link @malloy-publisher/sdk` in the app directory.
- If you linked the publisher sdk and make changes to it, you need to run `npm run build` in the main publisher directory to pick up the changes (probably best to restart the app too).
- Any print statements in the publisher sdk will show up in the browser console.
- When done, it's best to unlink the sdk in this app with `npm unlink @malloy-publisher/sdk` and remove the global link with `npm rm --global @malloy-publisher/sdk` run from anywhere.

