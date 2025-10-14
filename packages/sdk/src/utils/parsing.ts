// Helper function to extract a brief description from README content
export const getProjectDescription = (readme: string | undefined): string => {
   if (!readme) {
      return "Explore semantic models, run queries, and build dashboards";
   }

   // Remove markdown formatting and get first paragraph
   const cleanText = readme
      .replace(/^#+\s+/gm, "") // Remove headers
      .replace(/\*\*(.*?)\*\*/g, "$1") // Remove bold
      .replace(/\*(.*?)\*/g, "$1") // Remove italic
      .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1") // Remove links
      .replace(/`([^`]+)`/g, "$1") // Remove code
      .trim();

   // Get first paragraph (split by double newlines)
   const paragraphs = cleanText.split(/\n\s*\n/);
   const firstParagraph = paragraphs[1] || cleanText;

   // Limit to ~120 characters
   if (firstParagraph.length <= 120) {
      return firstParagraph;
   }

   // Truncate at word boundary
   const truncated = firstParagraph.substring(0, 120).split(" ");
   truncated.pop(); // Remove last partial word
   return truncated.join(" ") + "...";
};

export const generateProjectReadme = (
   project: {
      name: string;
      readme?: string;
   },
   description?: string,
): string => {
   const readmeLines = (project.readme || undefined)?.split("\n") ?? [];
   if (readmeLines.length === 0) {
      readmeLines.push(`# ${project.name}`);
      readmeLines.push("");
   }
   if ((project.readme?.length ?? 0) > 0 && description) {
      readmeLines.splice(2, 1, description);
   } else if (description) {
      readmeLines.push(description);
   }

   return readmeLines.join("\n");
};
