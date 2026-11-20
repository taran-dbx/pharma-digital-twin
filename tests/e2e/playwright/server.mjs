import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const port = Number(process.env.PORT ?? 4317);
const dist = fileURLToPath(new URL("../../../static/dist/", import.meta.url));
const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".webp": "image/webp",
};

function assetPath(pathname) {
  const relative = pathname.startsWith("/static/dist/")
    ? pathname.slice("/static/dist/".length)
    : pathname.slice(1);
  const candidate = normalize(join(dist, relative));
  return candidate.startsWith(dist) ? candidate : null;
}

createServer((request, response) => {
  const pathname = new URL(request.url ?? "/", `http://${request.headers.host}`).pathname;
  const candidate = assetPath(pathname);
  if (!candidate) {
    response.writeHead(403, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Forbidden");
    return;
  }
  const assetExists = candidate && existsSync(candidate) && statSync(candidate).isFile();
  if (!assetExists && extname(pathname)) {
    response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("Not found");
    return;
  }
  const file = assetExists ? candidate : join(dist, "index.html");

  response.writeHead(200, {
    "Content-Type": contentTypes[extname(file)] ?? "application/octet-stream",
    "Cache-Control": "no-store",
  });
  createReadStream(file).pipe(response);
}).listen(port, "127.0.0.1", () => {
  console.log(`Static SPA available at http://127.0.0.1:${port}`);
});
