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

export async function updateCard(id: string, front: string, back: string) {
  const card = await prisma.card.update({ where: { id }, data: { front, back } });
  revalidatePath(`/sets/${card.setId}`);
}

export async function renameGroup(id: string, name: string) {
  const group = await prisma.group.update({ where: { id }, data: { name } });
  revalidatePath(`/groups/${id}`);
  if (group.parentId) {
    revalidatePath(`/groups/${group.parentId}`);
  } else {
    revalidatePath("/");
  }
}

export async function renameSet(id: string, name: string) {
  const set = await prisma.set.update({ where: { id }, data: { name } });
  revalidatePath(`/sets/${id}`);
  revalidatePath(`/groups/${set.groupId}`);
}

async function collectGroupIds(rootId: string): Promise<string[]> {
  const ids = [rootId];
  const queue = [rootId];
  while (queue.length > 0) {
    const parentId = queue.shift()!;
    const children = await prisma.group.findMany({
      where: { parentId },
      select: { id: true },
    });
    for (const child of children) {
      ids.push(child.id);
      queue.push(child.id);
    }
  }
  return ids;
}

export async function deleteGroup(id: string) {
  const group = await prisma.group.findUnique({ where: { id }, select: { parentId: true } });
  const allGroupIds = await collectGroupIds(id);
  const sets = await prisma.set.findMany({
    where: { groupId: { in: allGroupIds } },
    select: { id: true },
  });
  const setIds = sets.map((s) => s.id);

  if (setIds.length > 0) {
    await prisma.$transaction([
      prisma.card.deleteMany({ where: { setId: { in: setIds } } }),
      prisma.set.deleteMany({ where: { groupId: { in: allGroupIds } } }),
      prisma.group.updateMany({ where: { id: { in: allGroupIds } }, data: { parentId: null } }),
      prisma.group.deleteMany({ where: { id: { in: allGroupIds } } }),
    ]);
  } else {
    await prisma.$transaction([
      prisma.set.deleteMany({ where: { groupId: { in: allGroupIds } } }),
      prisma.group.updateMany({ where: { id: { in: allGroupIds } }, data: { parentId: null } }),
      prisma.group.deleteMany({ where: { id: { in: allGroupIds } } }),
    ]);
  }

  if (group?.parentId) {
    revalidatePath(`/groups/${group.parentId}`);
  } else {
    revalidatePath("/");
  }
}

export async function deleteSet(id: string) {
  const set = await prisma.set.findUnique({ where: { id }, select: { groupId: true } });
  await prisma.$transaction([
    prisma.card.deleteMany({ where: { setId: id } }),
    prisma.set.delete({ where: { id } }),
  ]);
  if (set) {
    revalidatePath(`/groups/${set.groupId}`);
  }
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
