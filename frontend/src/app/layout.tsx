import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "O2C Context Graph",
  description: "Order-to-cash graph and conversational query interface",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
