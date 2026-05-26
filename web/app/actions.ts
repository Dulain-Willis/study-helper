"use server";

import { revalidatePath } from "next/cache";
import { prisma } from "@/lib/db";

export async function createGroup(formData: FormData) {
  const name = formData.get("name") as string;
  const parentId = (formData.get("parentId") as string) || null;

  await prisma.group.create({ data: { name, parentId } });

  if (parentId) {
    revalidatePath(`/groups/${parentId}`);
  } else {
    revalidatePath("/");
  }
}

export async function createSet(formData: FormData) {
  const name = formData.get("name") as string;
  const groupId = formData.get("groupId") as string;

  await prisma.set.create({ data: { name, groupId } });

  revalidatePath(`/groups/${groupId}`);
}

export async function addCard(formData: FormData) {
  const front = formData.get("front") as string;
  const back = formData.get("back") as string;
  const setId = formData.get("setId") as string;

  await prisma.card.create({ data: { front, back, setId } });

  revalidatePath(`/sets/${setId}`);
}

export async function mergeSets(formData: FormData) {
  const name = formData.get("name") as string;
  const groupId = formData.get("groupId") as string;
  const setIds = (formData.getAll("setIds") as string[]);

  const sourceSets = await prisma.set.findMany({
    where: { id: { in: setIds } },
    include: { cards: true },
  });

  const allCards = sourceSets.flatMap((s) =>
    s.cards.map((c) => ({ front: c.front, back: c.back }))
  );

  await prisma.set.create({
    data: {
      name,
      groupId,
      cards: { create: allCards },
    },
  });

  revalidatePath(`/groups/${groupId}`);
}
