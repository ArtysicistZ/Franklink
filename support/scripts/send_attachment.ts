/**
 * Send attachment via Photon SDK.
 *
 * Usage: npx ts-node send_attachment.ts <phone_number> <file_path> [file_name]
 *
 * Example: npx ts-node send_attachment.ts +14155551234 /path/to/image.jpg location-instructions.jpg
 */

import { AdvancedIMessageKit } from "@photon-ai/advanced-imessage-kit";

async function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error(
      "Usage: npx tsx send_attachment.ts <phone_number> <file_path> [file_name]"
    );
    process.exit(1);
  }

  const phoneNumber = args[0];
  const filePath = args[1];
  const fileName = args[2] || undefined;

  // Get API key and server URL from environment
  const apiKey = process.env.PHOTON_API_KEY;
  const serverUrl = process.env.PHOTON_SERVER_URL;

  if (!apiKey) {
    console.error("PHOTON_API_KEY environment variable is required");
    process.exit(1);
  }
  if (!serverUrl) {
    console.error("PHOTON_SERVER_URL environment variable is required");
    process.exit(1);
  }

  // Get or create SDK instance with server URL
  const sdk = AdvancedIMessageKit.getInstance({ apiKey, serverUrl });

  // Connect to Photon server
  console.error("Connecting to Photon...");
  await sdk.connect();
  console.error("Connected!");

  // Format chat GUID for iMessage
  const chatGuid = `iMessage;-;${phoneNumber}`;

  try {
    console.error(`Sending attachment to ${chatGuid}...`);
    const result = await sdk.attachments.sendAttachment({
      chatGuid,
      filePath,
      fileName,
    });

    console.log(JSON.stringify({ success: true, result }));
  } catch (error: any) {
    // Handle AggregateError to get all nested errors
    let errorDetails = error.message || String(error);
    if (error.errors && Array.isArray(error.errors)) {
      errorDetails = error.errors.map((e: any) => e.message || String(e)).join("; ");
    }
    if (error.cause) {
      errorDetails += ` | Cause: ${error.cause.message || String(error.cause)}`;
    }
    console.error(
      JSON.stringify({
        success: false,
        error: errorDetails,
        stack: error.stack,
      })
    );
    process.exit(1);
  }
}

main();
