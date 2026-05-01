const fs = require("fs");
const path = require("path");
const localtunnel = require(path.resolve("C:/Users/juddu/Downloads/PAM/.lt-runner/node_modules/localtunnel"));

async function main() {
  const port = parseInt(process.env.FULCRUM_TUNNEL_PORT || process.env.FULCRUM_PORT || "5067", 10);
  const requestedSubdomain = (process.env.FULCRUM_TUNNEL_SUBDOMAIN || "").trim();
  const outputFile = process.env.FULCRUM_TUNNEL_URL_FILE || "C:/Users/juddu/Downloads/PAM/fulcrum_tunnel_url.txt";

  const options = { port };
  if (requestedSubdomain) {
    options.subdomain = requestedSubdomain;
  }

  const tunnel = await localtunnel(options);
  fs.writeFileSync(outputFile, `${tunnel.url}\n`, "utf8");
  process.stdout.write(`${tunnel.url}\n`);

  const close = () => {
    try {
      tunnel.close();
    } catch (_) {
      // no-op
    }
    process.exit(0);
  };

  process.on("SIGINT", close);
  process.on("SIGTERM", close);
  tunnel.on("close", () => process.exit(0));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
