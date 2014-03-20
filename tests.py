import unittest

from fastdocgen import build_achievements


def py_build_achievements(alphabet):
    achievement = 256
    achievements = []
    for i, char in enumerate(alphabet[42:58]):
        achievement = (achievement + int(char, 16) * i) % 512
        if achievement < 256:
            achievements.append(achievement)
    return achievements


class FastDocGenTest(unittest.TestCase):

    ALPHABET = '0b1efc8985ca1efb7c1b56a8ec698b87fbdb7b27b6370af9782a48bb587019'

    def test_build_achievements(self):
        py = py_build_achievements(self.ALPHABET)
        c = build_achievements(self.ALPHABET)
        self.assertEqual(py, c)


if __name__ == '__main__':
    unittest.main()
