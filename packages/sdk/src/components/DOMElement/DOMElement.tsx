// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useRef } from "react";

interface DOMElement {
   element: HTMLElement;
}

export default function DOMElement({ element }: DOMElement) {
   const ref = useRef<HTMLDivElement>(null);

   useEffect(() => {
      const parent = ref.current;
      if (parent) {
         parent.innerHTML = "";
         parent.appendChild(element);
      }
   }, [element]);

   return <div ref={ref}></div>;
}
