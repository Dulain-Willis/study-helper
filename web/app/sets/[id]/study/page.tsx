import { notFound } from "next/navigation";
import { prisma } from "@/lib/db";
import StudyClient from "./study-client";

export default async function StudyPage(props: PageProps<"/sets/[id]/study">) {
  const { id } = await props.params;

  const set = await prisma.set.findUnique({
    where: { id },
    include: { cards: { orderBy: { createdAt: "asc" } } },
  });

  if (!set) notFound();
  if (set.cards.length === 0) notFound();

  return (
    <StudyClient
      setId={id}
      setName={set.name}
      cards={set.cards.map((c) => ({ id: c.id, front: c.front, back: c.back }))}
    />
  );
}
