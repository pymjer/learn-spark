def f(s:String) = "f(" + s + ")"
def g(s:String) = "g(" + s + ")"

val fComposeG = f _ compose g _

fComposeG("yay")

val two: PartialFunction[Int,String] = {case 2 => "two"}
val three: PartialFunction[Int,String] = {case 3 => "three"}
val four: PartialFunction[Int,String] = {case 4 => "four"}

val partial = two orElse three orElse four
partial(2)


def drop1[A](l: List[A]) = l.tail

drop1(List(1,2,3))

def head1[K](l: List[K]) = l.head
head1(List(4,5,6))

def t[T](v1:T) = v1

t(3)
t("hellow")

class Super[+A]
val sub:Super[AnyRef] = new Super[String]
// val sup:Super[String] = new Super[AnyRef] Error
class Animal { val sound = "rustle" }
class Bird extends Animal { override val sound = "call" }
class Chicken extends Bird { override val sound = "cluck" }

def sounds[T <: Animal](t:Seq[T]) = t.map(_.sound)

val flock = List(new Bird, new Bird)
val withChicken = new Chicken :: flock
new Animal :: flock

val names = List("LaoLei","WeiQing")
val namesWhicRK = "RK" :: names

def count(l:List[_]) = l.size
count(List(1,2,3,4,5))
count(List("aa"))

def hashcodes(f:Seq[_ <: AnyRef]) = f.hashCode()
hashcodes(Seq("String","aaa"))




