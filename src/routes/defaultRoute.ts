import express from "express";
import fs from "fs";
import path from "path";
import { marked } from "marked";

const router = express.Router();
const staticPath = path.join(__dirname, "../../public");
router.use("/public", express.static(staticPath));

router.get("/", (req, res) => {
  const readmePath = path.join(__dirname, "../..", "README.md");
  fs.readFile(readmePath, "utf8", (err, data) => {
    if (err) {
      console.error("Error reading README.md:", err);
      return res.status(500).send("Error loading README.md");
    }

    const htmlContent = marked(data);

    // Add CSS for dark mode and image resizing
    const styledHtml = `
      <html>
      <head>
        <style>
          /* General dark mode styles */
          body {
            background-color: #1a1a1a; /* Dark background */
            color: #ffffff; /* Light text */
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            line-height: 1.6;
          }
          a {
            color: #1e90ff; /* Light blue links */
            text-decoration: none;
          }
          a:hover {
            text-decoration: underline;
          }
          img {
            max-width: 100%;
            height: auto;
            display: block;
            margin: 20px auto;
          }
          pre {
            background-color: #1e1e1e; /* Darker background for code blocks */
            color: #dcdcdc; /* Light text for code */
            padding: 10px;
            border-radius: 5px;
            overflow-x: auto;
          }
          code {
            background-color: #1e1e1e;
            color: #dcdcdc;
            padding: 2px 4px;
            border-radius: 3px;
          }
          h1, h2, h3, h4, h5, h6 {
            color: #ffcc00; /* Highlighted headers */
          }
          blockquote {
            color: #cccccc; /* Light gray for quotes */
            border-left: 4px solid #ffcc00;
            padding-left: 10px;
            margin-left: 0;
            font-style: italic;
          }
        </style>
      </head>
      <body>
        ${htmlContent}
      </body>
      </html>
    `;

    res.setHeader("Content-Type", "text/html");
    res.send(styledHtml);
  });
});

export default router;
